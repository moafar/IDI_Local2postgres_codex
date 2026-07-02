# llee_centreprova

Contrato del flujo real de pruebas para lista de espera por centro y prueba.

## Entrada real

- Archivo: `data/input/test/llee_mensual.xlsx`
- Hoja: `LLEE per Centre i Prova`
- `header_row`: `1`
- Configuración: `config/flows/llee_centreprova.yml`

La lectura se valida contra el Excel real con `pandas` y `openpyxl`.
Actualmente el reader devuelve un `DataFrame` de texto con 252 filas y 15
columnas:

- `any`
- `mes`
- `Centre`
- `Proves`
- `Prestacions`
- `Pendents de programar`
- `% pendents programar`
- `Mes de 90 dies`
- `% mes 90 dies`
- `Entrades`
- `Sortides`
- `Diferència Entrades-Sortides`
- `Temps de demora (dies)`
- `Temps d'espera (dies)`
- `TD/TE (%)`

## Calidad esperada

La clave natural interna del fichero es:

- `any`
- `mes`
- `Centre`
- `Proves`

Las pruebas verifican sobre el fichero real que no existen filas duplicadas para
esa clave, no existen duplicados completos, y no quedan filas o columnas
completamente vacías tras la lectura. La configuración declara además
`processing.drop_empty_rows`, `processing.drop_empty_columns`,
`validation.required_columns`, `validation.duplicate_key` y
`validation.duplicate_policy`, que el runner aplica antes de escribir la salida
procesada.

La única transformación configurada para este flujo es
`processing.trim_strings: all`. Mantiene la lectura como texto y solo elimina
espacios exteriores accidentales antes de escribir la salida procesada.

Las políticas declarativas admitidas son `error`, `warning` y `report`. `error`
detiene el flujo, `warning` continúa y registra advertencia, y `report` continúa
dejando solo el dato en el reporte.

## Salidas

La salida procesada se declara como `output.processed_filename` y el reporte como
`output.report_filename`, ambos bajo `paths.output_base_dir` con compatibilidad
temporal para `paths.output_dir`.

Las pruebas del flujo verifican que se escriben ambas salidas, que el procesado
mantiene 252 filas y 15 columnas para el fichero real, y que el reporte incluye
hoja, `header_row`, clave de duplicados, conteo de duplicados y el estado
`postgresql: not_implemented`.
