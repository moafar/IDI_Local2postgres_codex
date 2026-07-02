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
`drop_empty_rows`, `drop_empty_columns` y `detect_duplicates`, que el runner aplica
antes de escribir la salida procesada.

La única transformación configurada para este flujo es `trim_strings` sobre todas
las columnas. Mantiene la lectura como texto y solo elimina espacios exteriores
accidentales antes de escribir la salida procesada.

## Salidas

La salida procesada esperada es `llee_centreprova_processed.csv` y el reporte
esperado es `llee_centreprova_report.json`, ambos bajo `paths.output_base_dir`
con compatibilidad temporal para `paths.output_dir`.

Las pruebas del flujo verifican que se escriben ambas salidas, que el procesado
mantiene 252 filas y 15 columnas para el fichero real, y que el reporte incluye
hoja, `header_row`, clave de duplicados, conteo de duplicados y el estado
`postgresql: not_implemented`.
