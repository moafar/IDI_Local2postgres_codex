# Conectar:

``` Bash
/usr/lib/postgresql/16/bin/pg_ctl \
  -D ~/postgres/data \
  -l ~/postgres/logs/postgresql.log \
  start
```

# Comprobar la conexion:

``` Bash
pg_isready -h 127.0.0.1 -p 5433
```