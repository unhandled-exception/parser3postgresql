Альтернативный драйвер Постгреса для Parser 3
=============================================

Форк драйвера Постгреса из Парсера для современных версий Постгреса (9.6 и выше). Строка подключения, не Парсеровская, а [стандартная Постгресовская в uri-формате](https://postgrespro.ru/docs/postgresql/13/libpq-connect#LIBPQ-CONNSTRING). Альтернативный драйвер не делает никаких конвертаций кодировок и автоматических транзакций. Если у вас до сих пор не весь код в utf-8, то посомтрите на параметр client_encoding в описании к libpq.

Собираем драйвер. На машине должны стоять dev-пакеты для Постгреса и компиляторы. Если вы смогли собрать Парсер и штатный драйвер, то и альтернативный соберется без проблем.

Кладем новый радом с Парсеровскийм драйвером в  и собрать:

```
> cd ~/parser3project/sql/
> git co https://github.com/unhandled-exception/parser3postgresql.git ./postgresql
> ./configure
> make
```

Драйвер лежит по пути ./lib/libparser3postgresql.so

Копируем драйвер в папку для парсеровский драйверов и в таблице драйверов в auto.p регистрируем на протокол postgresql:
```
$SQL[
	$.drivers[^table::create{protocol	driver	client
mysql	$sqldriversdir/libparser3mysql.so	libmysqlclient.so
postgresql	$sqldriversdir/libparser3postgresql.so	libpq.so
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
