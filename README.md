# up_to_postgresql

Proyecto para cargar archivos de datos en PostgreSQL, desarrollado desde cero con apoyo de agentes Codex.

## Estado

Base declarativa de configuración:

- `config/common.yml`: valores comunes.
- `config/env/test.yml` y `config/env/prd.yml`: overrides por entorno.
- `config/flows/*.yml`: definición declarativa de flujos.

El identificador de un flujo es el nombre del archivo sin extensión; por ejemplo,
`config/flows/clientes.yml` define el flujo `clientes`. Si el YAML declara `name`,
debe coincidir con ese identificador para que el descubrimiento y la resolución
`flows/<flow>.yml` usen el mismo contrato.

El CLI mantiene `--flow` y `--env` (`test` o `prd`) y valida que el flujo exista.
Con `--execute` resuelve la configuración por capas, ejecuta el procesamiento de
archivo configurado y genera una salida procesada con un reporte de ejecución. No
implementa carga en PostgreSQL.

La capa `readers` puede leer fuentes `csv` y `xlsx` desde configuración resuelta,
resolviendo rutas relativas con `paths.input_base_dir`. Devuelve un
`pandas.DataFrame` sin normalizar columnas ni convertir tipos.

El flujo `llee_centreprova` lee `data/input/test/llee_mensual.xlsx`, hoja
`LLEE per Centre i Prova`, elimina filas y columnas completamente vacías, detecta
duplicados internos con la clave `any`, `mes`, `Centre`, `Proves`, y escribe:

- `data/output/test/llee_centreprova_processed.csv`
- `data/output/test/llee_centreprova_report.json`

El flujo de prueba real `llee_centreprova` queda declarado en
`config/flows/llee_centreprova.yml` y se documenta en `docs/llee_centreprova.md`.
Sus pruebas cubren la lectura del archivo `data/input/test/llee_mensual.xlsx`,
la hoja `LLEE per Centre i Prova`, la fila de encabezado, texto, ausencia de
filas/columnas completamente vacías, duplicados internos, salida procesada y
reporte. PostgreSQL queda fuera del alcance actual y se reporta como
`not_implemented`.
