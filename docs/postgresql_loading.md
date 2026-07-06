# PostgreSQL Loading

La carga PostgreSQL es opcional y solo se ejecuta con `--load`.

Ejemplo en `test`:

```bash
export POSTGRES_PASSWORD='...'
python -m up_to_postgresql --flow llee_centreprova --env test --execute --load
```

Si `POSTGRES_PASSWORD` no existe, la aplicación pide la contraseña
interactivamente. La contraseña no debe guardarse en YAML ni en archivos
versionados.

## Configuración

Cada entorno declara conexión sin password:

- `host`
- `port`
- `database`
- `user`
- `schema`

Cada flujo cargable declara en `load`:

- `target_table`
- `load_mode`: `fail`, `replace`, `append` o `replace_partition`
- `partition_column` si `load_mode` es `replace_partition`
- `reload_existing_hash`
- `column_mapping` con pares `source` y `target`

El esquema destino deriva del entorno: `--env test` carga en `test` y
`--env prd` carga en `prd`.

## Comportamiento

Antes de modificar PostgreSQL se valida que la tabla exista, que las columnas
físicas coincidan con el mapping, que el DataFrame tenga las columnas origen y
que no haya destinos duplicados.

La carga se ejecuta en una transacción. Si falla, se revierte la modificación de
datos y se registra el fallo en `schema.load_control`.

`load_control` bloquea una carga si ya existe un `source_hash` exitoso para el
mismo `flow_name` y `target_table`, salvo que el flujo declare
`reload_existing_hash: true`.

El loader exige que `schema.load_control` exista y contenga estas columnas:
`flow_name`, `environment`, `source_filename`, `source_hash`, `target_schema`,
`target_table`, `load_mode`, `rows_read`, `rows_loaded`, `status`,
`finished_at` y `error_message`.

Modos:

- `fail`: falla si la tabla destino ya tiene filas.
- `replace`: borra filas de la tabla destino y carga todo.
- `append`: añade filas.
- `replace_partition`: carga primero en una tabla temporal, identifica los
  valores presentes en `partition_column`, borra solo esas particiones en la
  tabla destino e inserta los datos nuevos dentro de la misma transacción.
