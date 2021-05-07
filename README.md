Экспериментальный драйвер Постгреса для Parser 3
================================================

Драйвер не делает никаких конвертаций кодировок и автоматических транзакций. Строка подключения — [стандартная Постгресовская строка в uri-формате](https://postgrespro.ru/docs/postgresql/13/libpq-connect#LIBPQ-CONNSTRING).

В таблице драйверов регистрируем на протокол postgresql:
```
$SQL[
	$.drivers[^table::create{protocol	driver	client
mysql	$sqldriversdir/libparser3mysql.so	libmysqlclient.so
postgresql	$sqldriversdir/libparser3pgsql.so	libpq.so
oracle	$sqldriversdir/libparser3oracle.so	-configure could not guess-
sqlite	$sqldriversdir/libparser3sqlite.so	-configure could not guess-
}]
]
```

Соединение:
```
^connect[postgresql://host1:123,host2:456/somedb?target_session_attrs=any&application_name=myapp]{
    ...
}
```

------------------------

Parser3 PgSQL driver sources

Parser3 website is http://www.parser.ru/en/

Read the documentation at http://www.parser.ru/en/docs/

Check the Changelog to keep track of progresses.
Check the INSTALL to find out how to compile and install Parser3 PgSQL driver.

Report bugs to mailbox@parser.ru or on http://www.parser.ru/forum/

License https://www.parser.ru/en/download/license/
