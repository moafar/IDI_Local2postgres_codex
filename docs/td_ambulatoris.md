# td_ambulatoris

Flujo XLSX real para tiempos de demora ambulatorios.

- Archivo: `data/input/test/td.xlsx`
- Hoja: `td_ambulatoris`
- Configuración: `config/flows/td_ambulatoris.yml`
- Tabla destino: `td_ambulatoris`
- Salida procesada: `td_ambulatoris_processed.csv`
- Reporte: `td_ambulatoris_report.json`

El contrato de columnas procede de `docs/postgresql_mapping.md`. La lectura real
produce 12996 filas y 17 columnas, todas tratadas como texto. No se declara clave
natural porque el fichero contiene duplicados completos; el flujo los conserva y
los reporta mediante `duplicate_policy: report`.
