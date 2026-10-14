# demanda

Contrato del flujo de demanda: lee CSV y genera un CSV procesado y un reporte.
Depende de `config/flows/demanda.yml` y del mapeo PostgreSQL; las pruebas de
archivo usan datos sintéticos y no realizan cargas en la base de datos.

- Archivo configurado en `test`: `data/input/test/Demanda.csv`
- Formato: UTF-8, separador `;`, cabecera en la primera fila
- Configuración: `config/flows/demanda.yml`
- Tabla destino: `demanda`
- Salida procesada: `demanda_processed.csv`
- Reporte: `demanda_report.json`

El contrato contiene 23 columnas, leídas como texto. La clave declarada para
informar duplicados es `Data prestació`, `Prestació nivell 9 codi` y
`Pacient (NHC)`; no elimina registros. El archivo configurado no forma parte
del repositorio: las pruebas crean un CSV temporal con el mismo contrato.
