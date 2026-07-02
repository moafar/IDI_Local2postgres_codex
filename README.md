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

El CLI mantiene `--flow` y `--env` (`test` o `prd`), valida que el flujo exista y
resuelve la configuración por capas. No ejecuta lectores, transformaciones ni carga
en PostgreSQL.
