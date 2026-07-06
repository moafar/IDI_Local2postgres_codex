# llee_trams_demora

Flujo XLSX real para tramos de demora por centro y prueba.

- Archivo: `data/input/test/llee_mensual.xlsx`
- Hoja: `Trams demora per Centre i Prova`
- Configuración: `config/flows/llee_trams_demora.yml`
- Tabla destino: `llee_trams_demora`
- Salida procesada: `llee_trams_demora_processed.csv`
- Reporte: `llee_trams_demora_report.json`

El contrato de columnas procede de `docs/postgresql_mapping.md`. La lectura real
produce 473 filas y 15 columnas, todas tratadas como texto. La clave de
duplicados declarada es `any`, `mes`, `Centre`, `Prioritat` y
`Grup de monitorització`; en el fichero actual no hay duplicados para esa clave.
