# td_urgencies

Flujo XLSX real para tiempos de demora de urgencias.

- Archivo: `data/input/test/td.xlsx`
- Hoja: `td_urgencies`
- Configuración: `config/flows/td_urgencies.yml`
- Tabla destino: `td_urgencies`
- Salida procesada: `td_urgencies_processed.csv`
- Reporte: `td_urgencies_report.json`

El contrato de columnas procede de `docs/postgresql_mapping.md`. La lectura real
produce 588 filas y 8 columnas, todas tratadas como texto. La clave de duplicados
declarada es `solicitant`, `any`, `mes`, `episodi` y `prestació`; en el fichero
actual no hay duplicados para esa clave.
