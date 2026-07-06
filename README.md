# up_to_postgresql

Proyecto para cargar archivos CSV/XLSX hacia PostgreSQL mediante configuración declarativa.

## Resumen

El flujo de trabajo es:

1. Resolver configuración por capas.
2. Leer el fichero de origen.
3. Procesar y generar salida local.
4. Cargar en PostgreSQL solo si se pide explícitamente.

La configuración se compone de:

- `config/common.yml`
- `config/env/test.yml` o `config/env/prd.yml`
- `config/flows/<flow>.yml`

El identificador del flujo es el nombre del archivo YAML sin extensión y debe coincidir con `name` cuando se declara.

## Flujos reales

Los flujos reales actuales son:

- `activitat_actual`
- `llee_centreprova`
- `llee_trams_demora`
- `td_ambulatoris`
- `td_urgencies`
- `demanda`

Todos escriben salida procesada y reporte bajo `paths.output_base_dir`.

## CLI

Uso básico:

```bash
python -m up_to_postgresql --flow <flow> --env {test,prd} [--execute] [--load] [--source <ruta>]
```

Opciones:

- `--flow`: nombre del flujo.
- `--env`: entorno de ejecución, `test` o `prd`.
- `--execute`: ejecuta el procesamiento del fichero.
- `--load`: carga en PostgreSQL. Requiere `--execute`.
- `--source`: sustituye en memoria `source.path` para esta ejecución. La ruta es relativa a `paths.input_base_dir`.

Reglas relevantes:

- Sin `--execute`, el CLI solo resuelve la configuración del flujo.
- Sin `--load`, no se toca PostgreSQL.
- `--source` no modifica YAML, solo la configuración en memoria de esa ejecución.
- `--load` sin `--execute` se rechaza.

## Ejemplos

Procesamiento sin carga:

```bash
python -m up_to_postgresql --flow llee_centreprova --env test --execute
```

Carga con `--load`:

```bash
export POSTGRES_PASSWORD='...'
python -m up_to_postgresql --flow activitat_actual --env prd --execute --load
```

Carga usando `--source`:

```bash
export POSTGRES_PASSWORD='...'
python -m up_to_postgresql --flow llee_centreprova --env test --execute --load --source overrides/llee_mensual.xlsx
```

En este último caso el archivo indicado se resuelve respecto a `paths.input_base_dir` y solo afecta a esa ejecución.

## Rutas y ficheros

Las rutas de entrada y salida dependen del entorno y de la configuración final resuelta:

- `paths.input_base_dir`
- `paths.output_base_dir`

Por defecto:

- `config/common.yml` fija las rutas base comunes.
- `config/env/test.yml` y `config/env/prd.yml` las especializan por entorno.

Los directorios `data/input/**` y `data/output/**` no se versionan.

## Modos de carga

La carga PostgreSQL solo está disponible cuando se ejecuta con `--execute --load`.

Modos actuales:

- `append`: añade filas a la tabla destino.
- `fail`: solo carga si la tabla destino está vacía.
- `replace`: reemplaza completamente el contenido de la tabla destino.
- `replace_partition`: reemplaza únicamente las particiones detectadas por `partition_column`.

Cuándo aplica cada uno:

- `append` es el modo de `llee_centreprova`, `llee_trams_demora`, `td_ambulatoris` y `td_urgencies`.
- `fail` existe como modo soportado por el loader y se usa como control de seguridad cuando se quiere impedir sobreescritura si la tabla ya tiene datos.
- `replace` existe como modo soportado por el loader y se usa cuando se quiere rehacer por completo la tabla destino.
- `replace_partition` es el modo de `activitat_actual` y `demanda`.

Regla específica de `replace_partition`:

- `activitat_actual` usa `partition_column: any_prestacio`.
- `demanda` usa `partition_column: any_prestacio`.
- La carga borra solo las particiones presentes en el archivo de entrada y vuelve a insertar esas filas.

## PostgreSQL

PostgreSQL usa tablas físicas preexistentes en los esquemas `test` y `prd`.

La aplicación no crea tablas automáticamente. Antes de cargar valida que la tabla destino exista y que el contrato de columnas sea coherente.

La configuración de conexión se resuelve por entorno y no incluye la contraseña. Si `POSTGRES_PASSWORD` no está definida, la aplicación la solicita de forma interactiva.

Las referencias principales del contrato físico son:

- [docs/postgresql_mapping.md](docs/postgresql_mapping.md)
- [docs/postgresql_loading.md](docs/postgresql_loading.md)

## Estado operativo `prd`

Estado validado de carga en `prd`:

- `activitat_actual`: 1.499.053 filas
  - `2025`: 986.124
  - `2026`: 512.929
- `llee_centreprova` / `llee_centre_prova`: 315 filas
- `llee_trams_demora`: 591 filas
- `td_ambulatoris`: 12.996 filas
- `td_urgencies`: 610 filas
- `demanda`: 0 filas

Incidencias documentables:

- `td_ambulatoris` `202605` no se cargó porque el archivo disponible no contenía las 17 columnas requeridas.
- `td_urgencies` `202605` se cargó desde `td_202605_normalized.xlsx`, generado a partir de la hoja `td2025`.
- `td_urgencies` no conserva coordenadas: si el origen incluye `lat` o `lon`, no forman parte del contrato de carga PostgreSQL.
- `demanda` todavía no tiene carga real en `prd`.

## Nota sobre `llee_centreprova`

`llee_centreprova` es uno de los flujos reales del proyecto, pero no el único flujo principal. El conjunto operativo completo es el listado en la sección de flujos reales.
