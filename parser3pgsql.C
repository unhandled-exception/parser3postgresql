/** @file
	Parser PgSQL driver.

	Copyright (c) 2001-2019 Art. Lebedev Studio (http://www.artlebedev.com)

	Author: Alexandr Petrosian <paf@design.ru> (http://paf.design.ru)

	2007.10.25 using PgSQL 8.1.5
*/

#include "config_includes.h"

#include "pa_sql_driver.h"

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

volatile const char * IDENT_PARSER3PGSQL_C="$Id: parser3pgsql.C,v 1.46 2019/11/30 22:11:11 moko Exp $" IDENT_PA_SQL_DRIVER_H;

// from catalog/pg_type.h
#define BOOLOID			16
#define INT8OID			20
#define INT2OID			21
#define INT4OID			23
#define OIDOID			26
#define FLOAT4OID		700
#define FLOAT8OID		701
#define DATEOID			1082
#define TIMEOID			1083
#define TIMESTAMPOID	1114
#define TIMESTAMPTZOID	1184
#define TIMETZOID		1266
#define NUMERICOID		1700

// LO_BUFSIZE from interfaces\libpq\fe-lobj.c = 8192 (0x2000)
// actually writing chunks of that size failed, reduced it twice
#define LO_BUFSIZE		  0x1000


#include "ltdl.h"

#define MAX_COLS 500

#define MAX_STRING 0x400
#define MAX_NUMBER 20

#define PG_CONNECT_STRING_PREFIX "postgresql://"

#if _MSC_VER
#	define snprintf _snprintf
#	define strcasecmp _stricmp
#endif

#ifndef max
inline int max(int a,int b){ return a>b?a:b; }
inline int min(int a,int b){ return a<b?a:b; }
#endif

static char *lsplit(char *string, char delim){
	if(string){
		if(char *v=strchr(string, delim)){
			*v=0;
			return v+1;
		}
	}
	return 0;
}

static char *lsplit(char **string_ref, char delim){
	char *result=*string_ref;
	char *next=lsplit(*string_ref, delim);
	*string_ref=next;
	return result;
}

static char* rsplit(char* string, char delim){
	if(string){
		if(char* v=strrchr(string, delim)){
			*v=0;
			return v+1;
		}
	}
	return NULL;
}

static void toupper_str(char *out, const char *in, size_t size){
	while(size--)
		*out++=(char)toupper(*in++);
}

inline static const char* strdup(SQL_Driver_services& services, char* str, size_t length) {
	char *strm=(char*)services.malloc_atomic(length+1);
	memcpy(strm, str, length);
	strm[length]=0;
	return (const char*)strm;
}

struct Connection {
	SQL_Driver_services* services;

	PGconn *conn;
	const char* client_charset;
};

/**
	PgSQL server driver
*/
class PgSQL_Driver : public SQL_Driver {
public:

	PgSQL_Driver() : SQL_Driver() {
	}

	/// get api version
	int api_version(){ return SQL_DRIVER_API_VERSION; }

	/// initialize driver by loading sql dynamic link library
	const char *initialize(char *dlopen_file_spec){
		return dlopen_file_spec?
			dlink(dlopen_file_spec):"client library column is empty";
	}

	#define throwPQerror connection.services->_throw(PQerrorMessage(connection.conn))
	#define PQclear_throw(msg) { \
			PQclear(res); \
			connection.services->_throw(msg); \
		}
	#define PQclear_throwPQerror PQclear_throw(PQerrorMessage(connection.conn))

	/**	connect
		@param url See https://postgrespro.ru/docs/postgresql/13/libpq-connect#LIBPQ-CONNSTRING
	*/
	void connect(
				char* url,
				SQL_Driver_services& services,
				void** connection_ref ///< output: Connection*
	){
		Connection& connection=*(Connection *)services.malloc(sizeof(Connection));

		*connection_ref=&connection;
		connection.services=&services;
		connection.client_charset=0;

		char *pg_url = (char*)connection.services->malloc_atomic(strlen(url) + strlen(PG_CONNECT_STRING_PREFIX) + 1);
		*pg_url = 0;
		strcat(pg_url, PG_CONNECT_STRING_PREFIX);
		strcat(pg_url, url);

		connection.conn=PQconnectdb(pg_url);

		if(!connection.conn)
			services._throw("PQconnectdb failed");

		if(PQstatus(connection.conn)!=CONNECTION_OK)
			throwPQerror;
	}

	void disconnect(void *aconnection){
		Connection& connection=*static_cast<Connection*>(aconnection);
		PQfinish(connection.conn);
		connection.conn=0;
	}

	void commit(void *aconnection){}

	void rollback(void *aconnection){}

	bool ping(void *aconnection) {
		Connection& connection=*static_cast<Connection*>(aconnection);
		return PQstatus(connection.conn)==CONNECTION_OK;
	}

	// charset here is services.request_charset(), not connection.client_charset
	// thus we can't use the sql server quoting support
	const char* quote(void *aconnection, const char *str, unsigned int length)
	{
		Connection& connection=*static_cast<Connection*>(aconnection);

		const char* from;
		const char* from_end=str+length;

		size_t quoted=0;

		for(from=str; from<from_end; from++){
			if(*from=='\'')
				quoted++;
		}

		if(!quoted)
			return str;

		char *result=(char*)connection.services->malloc_atomic(length + quoted + 1);
		char *to = result;

		for(from=str; from<from_end; from++){
			if(*from=='\'')
				*to++= '\''; // "'" -> "''"
			*to++=*from;
		}

		*to=0;
		return result;
	}

	void query(void *aconnection,
				const char *astatement,
				size_t placeholders_count, Placeholder* placeholders,
				unsigned long offset, unsigned long limit,
				SQL_Driver_query_event_handlers& handlers
	){
		Connection& connection=*static_cast<Connection*>(aconnection);
		SQL_Driver_services& services=*connection.services;
		PGconn *conn=connection.conn;

		const char* client_charset=connection.client_charset;
		const char* request_charset=services.request_charset();

		const char** paramValues;
		if(placeholders_count>0){
			int binds_size=sizeof(char)*placeholders_count;
			paramValues = static_cast<const char**>(services.malloc_atomic(binds_size));
			_bind_parameters(placeholders_count, placeholders, paramValues, connection);
		}

		size_t statement_size=0;

		const char *statement=_preprocess_statement(connection, astatement, statement_size, offset, limit);
		// error after prepare?

		PGresult *res;
		if(placeholders_count>0){
			res=PQexecParams(conn, statement, placeholders_count, NULL, paramValues, NULL, NULL, 0);
		} else {
			res=PQexec(conn, statement);
		}
		if(!res)
			throwPQerror;

		bool failed=false;
		SQL_Error sql_error;

		switch(PQresultStatus(res)) {
			case PGRES_EMPTY_QUERY:
				PQclear_throw("no query");
				break;
			case PGRES_COMMAND_OK: // empty result: insert|delete|update|...
				PQclear(res);
				return;
			case PGRES_TUPLES_OK:
				break;
			default:
				PQclear_throwPQerror;
				break;
		}

#define CHECK(afailed) \
		if(afailed) { \
			failed=true; \
			goto cleanup; \
		}

		size_t column_count=PQnfields(res);
		if(!column_count)
			PQclear_throw("result contains no columns");

		if(column_count>MAX_COLS)
			column_count=MAX_COLS;

		unsigned int column_types[MAX_COLS];

		for(size_t i=0; i<column_count; i++){
			column_types[i]=PQftype(res, i);

			char *name=PQfname(res, i);
			size_t length=strlen(name);
			const char* str=strdup(services, name, length);

			CHECK(handlers.add_column(sql_error, str, length));
		}

		CHECK(handlers.before_rows(sql_error));

		if(unsigned long row_count=(unsigned long)PQntuples(res))
			for(unsigned long r=0; r<row_count; r++) {
				CHECK(handlers.add_row(sql_error));
				for(size_t i=0; i<column_count; i++){
					char *cell=PQgetvalue(res, r, i);

					size_t length=0;
					const char* str;

					switch(column_types[i]){
						case BOOLOID:
						case INT8OID:
						case INT2OID:
						case INT4OID:
						case FLOAT4OID:
						case FLOAT8OID:
						case DATEOID:
						case TIMEOID:
						case TIMESTAMPOID:
						case TIMESTAMPTZOID:
						case TIMETZOID:
						case NUMERICOID:
							length=(size_t)PQgetlength(res, r, i);
							str=length ? strdup(services, cell, length) : 0;
							break;
						case OIDOID:
							{
								Oid oid=cell?atoi(cell):0;
								int fd=lo_open(conn, oid, INV_READ);
								if(fd>=0){
									// seek to end
									if(lo_lseek(conn, fd, 0, SEEK_END)<0)
										PQclear_throwPQerror;
									// get length
									int size_tell=lo_tell(conn, fd);
									if(size_tell<0)
										PQclear_throwPQerror;
									// seek to begin
									if(lo_lseek(conn, fd, 0, SEEK_SET)<0)
										PQclear_throwPQerror;
									length=(size_t)size_tell;
									if(length){
										// read
										char* strm=(char*)services.malloc(length+1);
										if(!lo_read_ex(conn, fd, strm, size_tell))
											PQclear_throw("lo_read can not read all bytes of object");
										strm[length]=0;
										str=strm;
									} else
										str=0;
									if(lo_close(conn, fd)<0)
										PQclear_throwPQerror;
								} else
									PQclear_throwPQerror;
								break;
							}
						default:
							// normal column, read it normally
							length=(size_t)PQgetlength(res, r, i);
							str=length ? strdup(services, cell, length) : 0;
							break;
					}
					CHECK(handlers.add_row_cell(sql_error, str, length));
				}
			}
cleanup:
		PQclear(res);
		if(failed)
			services._throw(sql_error);
	}

private:
	void _bind_parameters(
				size_t placeholders_count,
				Placeholder* placeholders,
				const char** paramValues,
				Connection& connection
	){
		for(size_t i=0; i<placeholders_count; i++){
			Placeholder& ph=placeholders[i];
			int name_number=atoi(ph.name);
			if(name_number <= 0 || (size_t)name_number > placeholders_count)
				connection.services->_throw("bad bind parameter key");

			paramValues[name_number-1]=ph.value;
		}
	}

	// executes a query and throw away the result.
	void _execute_cmd(const Connection& connection, const char *query){
		if(PGresult *res=PQexec(connection.conn, query))
			PQclear(res); // throw away the result [don't need but must call]
		else
			throwPQerror;
	}

	const char *_preprocess_statement(
					Connection& connection,
					const char *astatement,
					size_t statement_size,
					unsigned long offset,
					unsigned long limit
	){
		PGconn *conn=connection.conn;

		if(!statement_size)
			statement_size=strlen(astatement);

		char *result=(char *)connection.services->malloc(statement_size
			+MAX_NUMBER*2+15 // " limit # offset #"
			+MAX_STRING // in case of short 'strings'
			+1);
		// offset & limit -> suffixes
		const char *o;
		if(offset || limit!=SQL_NO_LIMIT){
			char *cur=result;
			memcpy(cur, astatement, statement_size); cur+=statement_size;
			if(limit!=SQL_NO_LIMIT)
				cur+=snprintf(cur, 7+MAX_NUMBER, " limit %lu", limit);
			if(offset)
				cur+=snprintf(cur, 8+MAX_NUMBER, " offset %lu", offset);
			o=result;
		} else
			o=astatement;

		// /**xxx**/'literal' -> oid
		char *n=result;
		while(*o) {
			if(
				o[0]=='/' &&
				o[1]=='*' &&
				o[2]=='*') { // name start
				const char* saved_o=o;
				o+=3;
				while(*o)
					if(
						o[0]=='*' &&
						o[1]=='*' &&
						o[2]=='/' &&
						o[3]=='\'') { // name end
						saved_o=0; // found, marking that
						o+=4;
						Oid oid=lo_creat(conn, INV_READ|INV_WRITE);
						if(oid==InvalidOid)
							throwPQerror;
						int fd=lo_open(conn, oid, INV_WRITE);
						if(fd>=0) {
							const char *start=o;
							bool escaped=false;
							while(*o && !(o[0]=='\'' && o[1]!='\'' && !escaped)) {
								escaped=*o=='\\' || (o[0]=='\'' && o[1]=='\'');
								if(escaped) {
									// write pending, skip "\" or "'"
									if(!lo_write_ex(conn, fd, start, o-start))
										connection.services->_throw("lo_write could not write all bytes of object (1)");
									start=++o;
								} else
									o++;
							}
							if(!lo_write_ex(conn, fd, start, o-start))
								connection.services->_throw("lo_write can not write all bytes of object (2)");
							if(lo_close(conn, fd)<0)
								throwPQerror;
						} else
							throwPQerror;
						if(*o)
							o++; // skip "'"

						n+=snprintf(n, MAX_NUMBER, "%u", oid);
						break;
					} else
						o++; // /**skip**/'xxx'
				if(saved_o) {
					o=saved_o;
					*n++=*o++;
				}
			} else
				*n++=*o++;
		}
		*n=0;

		return result;
	}

private: // lo_read/write exchancements

	bool lo_read_ex(PGconn *conn, int fd, const/*paf*/ char *buf, size_t len) {
		return lo_rw_method (conn, fd, buf, len, lo_read);
	}

	bool lo_write_ex(PGconn *conn, int fd, const/*paf*/ char *buf, size_t len) {
		return lo_rw_method (conn, fd, buf, len, lo_write);
	}

	bool lo_rw_method(PGconn *conn, int fd, const/*paf*/ char *buf, size_t len, int (*lo_func)(PGconn *conn, int fd, const/*paf*/ char *buf, size_t len)) {
		int size_op;
		while(len && (size_op=lo_func(conn, fd, buf, min(LO_BUFSIZE, len)))>0) {
			buf+=size_op;
			len-=size_op;
		}
		return len==0;
	}

private: // conn client library funcs
	typedef PGconn* (*t_PQconnectdb)(const char *conninfo); t_PQconnectdb PQconnectdb;
	typedef void (*t_PQfinish)(PGconn *conn);  t_PQfinish PQfinish;
	typedef char *(*t_PQerrorMessage)(const PGconn* conn); t_PQerrorMessage PQerrorMessage;
	typedef ConnStatusType (*t_PQstatus)(const PGconn *conn); t_PQstatus PQstatus;
	typedef PGresult *(*t_PQexec)(PGconn *conn,
						const char *query); t_PQexec PQexec;
	typedef PGresult *(*t_PQexecParams)(
						PGconn *conn,
						const char *query,
						int nParams,
						const Oid *paramTypes,
						const char * const *paramValues,
						const int *paramLengths,
						const int *paramFormats,
						int resultFormat); t_PQexecParams PQexecParams;

	typedef ExecStatusType (*t_PQresultStatus)(const PGresult *res); t_PQresultStatus PQresultStatus;
	typedef int (*t_PQgetlength)(const PGresult *res,
						int tup_num,
						int field_num); t_PQgetlength PQgetlength;
	typedef char* (*t_PQgetvalue)(const PGresult *res,
						int tup_num,
						int field_num); t_PQgetvalue PQgetvalue;
	typedef int	(*t_PQntuples)(const PGresult *res); t_PQntuples PQntuples;
	typedef char *(*t_PQfname)(const PGresult *res,
						int field_index); t_PQfname PQfname;
	typedef int (*t_PQnfields)(const PGresult *res); t_PQnfields PQnfields;
	typedef void (*t_PQclear)(PGresult *res); t_PQclear PQclear;

	typedef Oid	(*t_PQftype)(const PGresult *res, int field_num); t_PQftype PQftype;

	typedef size_t (*t_PQescapeStringConn)(PGconn *conn,
						char *to, const char *from, size_t length,
						int *error); t_PQescapeStringConn PQescapeStringConn;

	typedef int	(*t_lo_open)(PGconn *conn, Oid lobjId, int mode); t_lo_open lo_open;
	typedef int	(*t_lo_close)(PGconn *conn, int fd); t_lo_close lo_close;
	typedef int	(*t_lo_read)(PGconn *conn, int fd, const/*paf*/ char *buf, size_t len); t_lo_read lo_read;
	typedef int	(*t_lo_write)(PGconn *conn, int fd, const/*paf*/ char *buf, size_t len); t_lo_write lo_write;
	typedef int	(*t_lo_lseek)(PGconn *conn, int fd, int offset, int whence); t_lo_lseek lo_lseek;
	typedef Oid	(*t_lo_creat)(PGconn *conn, int mode); t_lo_creat lo_creat;
	typedef int	(*t_lo_tell)(PGconn *conn, int fd); t_lo_tell lo_tell;
	typedef int	(*t_lo_unlink)(PGconn *conn, Oid lobjId); t_lo_unlink lo_unlink;
	typedef Oid	(*t_lo_import)(PGconn *conn, const char *filename); t_lo_import lo_import;
	typedef int	(*t_lo_export)(PGconn *conn, Oid lobjId, const char *filename); t_lo_export lo_export;

private: // conn client library funcs linking

	const char *dlink(const char *dlopen_file_spec) {
		if(lt_dlinit()){
			if(const char* result=lt_dlerror())
				return result;
			return "can not prepare to dynamic loading";
		}

		lt_dlhandle handle=lt_dlopen(dlopen_file_spec);

		if(!handle){
			if(const char* result=lt_dlerror())
				return result;
			return "can not open the dynamic link module";
		}

		#define DSLINK(name, action) \
			name=(t_##name)lt_dlsym(handle, #name); \
				if(!name) \
					action;

		#define DLINK(name) DSLINK(name, return "function " #name " was not found")

		DLINK(PQconnectdb);
		DLINK(PQerrorMessage);
		DLINK(PQstatus);
		DLINK(PQfinish);
		DLINK(PQgetvalue);
		DLINK(PQgetlength);
		DLINK(PQntuples);
		DLINK(PQfname);
		DLINK(PQnfields);
		DLINK(PQclear);
		DLINK(PQresultStatus);
		DLINK(PQexec);
		DLINK(PQexecParams);
		DLINK(PQftype);
		DLINK(PQescapeStringConn);
		DLINK(lo_open);		DLINK(lo_close);
		DLINK(lo_read);		DLINK(lo_write);
		DLINK(lo_lseek);		DLINK(lo_creat);
		DLINK(lo_tell);		DLINK(lo_unlink);
		DLINK(lo_import);		DLINK(lo_export);

		return 0;
	}
};

extern "C" SQL_Driver *SQL_DRIVER_CREATE() {
	return new PgSQL_Driver();
}
