# activitat_actual

Contrato del segundo flujo real de pruebas para actividad actual.

## Entrada real

- Archivo: `data/input/test/activitat_actual_2006-ene-may__.csv`
- Formato: CSV UTF-8 con delimitador `;`
- `header_row`: `1`
- Configuración: `config/flows/activitat_actual.yml`

La lectura se valida contra el CSV real con `pandas`. El reader conserva todos
los campos como texto (`dtype=str`) y no convierte fechas, horas, códigos ni
decimales con coma. El fichero contiene 423933 filas de datos y 46 columnas.

## Calidad esperada

La configuración declara todas las columnas reales como `required_columns` y
usa `missing_columns_policy: error`.

No hay una clave natural única justificada para este fichero dentro del alcance
actual. Por ello `validation.duplicate_key` declara las 46 columnas reales, de
forma que la detección de duplicados se aplica sobre filas completas con
`duplicate_policy: report`. En el fichero real no se han encontrado duplicados
completos, ni filas o columnas completamente vacías.

La única transformación configurada es `processing.trim_strings: all`. Mantiene
la lectura como texto y elimina espacios exteriores accidentales antes de
escribir la salida procesada.

## Salidas

El flujo escribe bajo `paths.output_base_dir`:

- `activitat_actual_processed.csv`
- `activitat_actual_report.json`

El reporte incluye métricas de filas y columnas leídas/escritas, filas y
columnas vacías eliminadas, columnas requeridas, duplicados completos,
transformaciones aplicadas y `postgresql: not_implemented`.
