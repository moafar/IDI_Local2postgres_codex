# Readers

Contrato mínimo esperado para `up_to_postgresql.readers.ReaderFactory`:

- `source["type"]` acepta `csv` y `xlsx`.
- `source["path"]` apunta al archivo de entrada y debe existir.
- `ReaderFactory.create(config).read()` devuelve un `pandas.DataFrame`.
- CSV se lee con encabezado en la primera fila por defecto.
- XLSX se lee desde la primera hoja por defecto.
- `source["sheet"]` selecciona una hoja XLSX concreta.
- `source["header_row"]` configura la fila de encabezado con índice base uno; el
  valor `1` usa la primera fila.
- Los valores se leen como texto (`dtype=str`) y las celdas vacías se conservan
  como cadena vacía.
- Un archivo inexistente debe propagar `FileNotFoundError`.
- Un `source["type"]` no soportado debe producir `ReaderError`.

`pandas` y `openpyxl` son dependencias de ejecución para poder leer y validar CSV
y XLSX en pruebas.

## Flujo LLEE centre/prova

`llee_centreprova` usa el archivo `llee_mensual.xlsx` del entorno `test` y la hoja
`LLEE per Centre i Prova`. La ejecución con `--execute` conserva las columnas como
texto, elimina filas y columnas completamente vacías, detecta duplicados internos
por `any`, `mes`, `Centre`, `Proves`, y genera un CSV procesado más un reporte JSON.

El flujo real `llee_centreprova` tiene documentación específica en
`docs/llee_centreprova.md`.
