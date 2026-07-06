# demanda

Flujo XLSX real para demanda.

- Archivo: `data/input/test/demanda._samplexlsx.xlsx`
- Hoja: `Demanda`
- Configuración: `config/flows/demanda.yml`
- Tabla destino: `demanda`
- Salida procesada: `demanda_processed.csv`
- Reporte: `demanda_report.json`

El contrato de columnas procede de `docs/postgresql_mapping.md`. La lectura real
produce 9 filas y 23 columnas, todas tratadas como texto. La clave de duplicados
declarada es `Data prestació`, `Prestació nivell 9 codi` y `Pacient (NHC)`; en
el fichero actual no hay duplicados para esa clave.
