# PostgreSQL Mapping

Contrato físico objetivo para la futura carga en PostgreSQL.

## Alcance

- Base de datos: `data_dbo_idi`
- Esquema de validación: `test`
- Esquema de producción: `prd`
- `--env test` carga en el esquema `test`.
- `--env prd` carga en el esquema `prd`.

## Reglas generales

- Las tablas destino existen previamente en PostgreSQL.
- La aplicación no debe crear tablas automáticamente.
- Todas las columnas de negocio se cargan inicialmente como `TEXT`.
- Los nombres físicos son técnicos, `snake_case`, sin acentos ni espacios.
- El pipeline debe mapear explícitamente columna de origen a columna PostgreSQL.
- La contraseña PostgreSQL no debe almacenarse en archivos versionados.
- Los archivos de datos en `data/input/**` y `data/output/**` no deben versionarse.

## Tablas por entorno

Las siguientes tablas existen en ambos esquemas, `test` y `prd`:

- `activitat_actual`
- `demanda`
- `llee_centre_prova`
- `llee_trams_demora`
- `load_control`
- `td_ambulatoris`
- `td_urgencies`

## Mapeo de columnas

### `activitat_actual`

Flujo CSV real de actividad actual.

- Archivo de referencia: `data/input/test/activitat_actual_2006-ene-may__.csv`

| orden | columna_origen | columna_postgresql | tipo_postgresql |
| ---: | --- | --- | --- |
| 1 | `Any prestació (YYYY)` | `any_prestacio` | `TEXT` |
| 2 | `Centre SAP codi (Hospital)` | `centre_sap_codi` | `TEXT` |
| 3 | `Centre SAP desc (Hospital)` | `centre_sap_descripcio` | `TEXT` |
| 4 | `Data prestació` | `data_prestacio` | `TEXT` |
| 5 | `Dia setmana prestació (desc)` | `dia_setmana_prestacio` | `TEXT` |
| 6 | `Hora prestació` | `hora_prestacio` | `TEXT` |
| 7 | `Institució` | `institucio` | `TEXT` |
| 8 | `Interlocutor/Metge sol·licitant codi (ordre)` | `metge_solicitant_codi` | `TEXT` |
| 9 | `Interlocutor/Metge sol·licitant desc (ordre)` | `metge_solicitant_descripcio` | `TEXT` |
| 10 | `Màquina de la prestació codi` | `maquina_prestacio_codi` | `TEXT` |
| 11 | `Màquina de la prestació desc` | `maquina_prestacio_descripcio` | `TEXT` |
| 12 | `Mes prestació (MM)` | `mes_prestacio` | `TEXT` |
| 13 | `Metge Radiologia Docum. Allibera codi` | `metge_radiologia_allibera_codi` | `TEXT` |
| 14 | `Metge Radiologia Docum. Allibera desc` | `metge_radiologia_allibera_descripcio` | `TEXT` |
| 15 | `Nivell 1 codi` | `nivell_1_codi` | `TEXT` |
| 16 | `Nivell 1 desc` | `nivell_1_descripcio` | `TEXT` |
| 17 | `Pacient (NHC)` | `pacient_nhc` | `TEXT` |
| 18 | `Prestació nivell 9 codi` | `prestacio_nivell_9_codi` | `TEXT` |
| 19 | `Prestació nivell 9 desc` | `prestacio_nivell_9_descripcio` | `TEXT` |
| 20 | `Servei sol·licitant codi (ordre)` | `servei_solicitant_codi` | `TEXT` |
| 21 | `Servei sol·licitant desc (ordre)` | `servei_solicitant_descripcio` | `TEXT` |
| 22 | `Tipus episodi codi` | `tipus_episodi_codi` | `TEXT` |
| 23 | `UP sol·licitant codi` | `up_solicitant_codi` | `TEXT` |
| 24 | `UP sol·licitant desc` | `up_solicitant_descripcio` | `TEXT` |
| 25 | `UP sol·licitant entitat proveïdora codi` | `up_solicitant_entitat_proveidora_codi` | `TEXT` |
| 26 | `UP sol·licitant entitat proveïdora desc` | `up_solicitant_entitat_proveidora_descripcio` | `TEXT` |
| 27 | `UP sol·licitant tipus` | `up_solicitant_tipus` | `TEXT` |
| 28 | `UT Gestora codi` | `ut_gestora_codi` | `TEXT` |
| 29 | `UT Gestora desc` | `ut_gestora_descripcio` | `TEXT` |
| 30 | `UT sol·licitant codi` | `ut_solicitant_codi` | `TEXT` |
| 31 | `UT sol·licitant desc` | `ut_solicitant_descripcio` | `TEXT` |
| 32 | `Número de prestacions` | `numero_prestacions` | `TEXT` |
| 33 | `Temps entre ordre clínica i data de prestació realitzada (dies)` | `dies_ordre_a_prestacio_realitzada` | `TEXT` |
| 34 | `Temps entre ordre clínica i data de Realitazacio Informe (dies)` | `dies_ordre_a_realitzacio_informe` | `TEXT` |
| 35 | `Temps entre ordre clínica i data informe (dies)` | `dies_ordre_a_informe` | `TEXT` |
| 36 | `Temps entre prestació realitzada i data d'informe (dies)` | `dies_prestacio_realitzada_a_informe` | `TEXT` |
| 37 | `v_Centre Codi` | `centre_codi` | `TEXT` |
| 38 | `v_Direcció Clínica` | `direccio_clinica` | `TEXT` |
| 39 | `v_Institució corregida` | `institucio_corregida` | `TEXT` |
| 40 | `v_Màquina descripció` | `maquina_descripcio` | `TEXT` |
| 41 | `v_Prestació descripció` | `prestacio_descripcio` | `TEXT` |
| 42 | `v_Resum Sol·licitant` | `resum_solicitant` | `TEXT` |
| 43 | `v_Torn` | `torn` | `TEXT` |
| 44 | `v_UP Sol·licitant Corregida` | `up_solicitant_corregida` | `TEXT` |
| 45 | `v_UP Sol·licitant Entitat Corregida` | `up_solicitant_entitat_corregida` | `TEXT` |
| 46 | `v_UT Gestora corregida` | `ut_gestora_corregida` | `TEXT` |

### `demanda`

Tabla de demanda.

- Archivo de referencia: `data/input/test/demanda._samplexlsx.xlsx`
- Hoja: `Demanda`

| orden | columna_origen | columna_postgresql | tipo_postgresql |
| ---: | --- | --- | --- |
| 1 | `Any prestació (YYYY)` | `any_prestacio` | `TEXT` |
| 2 | `Mes prestació (MM)` | `mes_prestacio` | `TEXT` |
| 3 | `Data prestació` | `data_prestacio` | `TEXT` |
| 4 | `Número de prestacions` | `numero_prestacions` | `TEXT` |
| 5 | `Centre SAP codi (Hospital)` | `centre_sap_codi` | `TEXT` |
| 6 | `Centre Codi` | `centre_codi` | `TEXT` |
| 7 | `Institució` | `institucio` | `TEXT` |
| 8 | `Institució corregida` | `institucio_corregida` | `TEXT` |
| 9 | `Màquina de la prestació codi` | `maquina_prestacio_codi` | `TEXT` |
| 10 | `Màquina de la prestació desc` | `maquina_prestacio_descripcio` | `TEXT` |
| 11 | `Màquina Descripció` | `maquina_descripcio` | `TEXT` |
| 12 | `Prestació descripció` | `prestacio_descripcio` | `TEXT` |
| 13 | `Prestació nivell 9 codi` | `prestacio_nivell_9_codi` | `TEXT` |
| 14 | `Prestació nivell 9 desc` | `prestacio_nivell_9_descripcio` | `TEXT` |
| 15 | `Nivell 1 codi` | `nivell_1_codi` | `TEXT` |
| 16 | `Nivell 1 desc` | `nivell_1_descripcio` | `TEXT` |
| 17 | `UP sol·licitant desc` | `up_solicitant_descripcio` | `TEXT` |
| 18 | `UP sol·licitant entitat proveïdora desc` | `up_solicitant_entitat_proveidora_descripcio` | `TEXT` |
| 19 | `UP Sol·licitant Corregida` | `up_solicitant_corregida` | `TEXT` |
| 20 | `UP Sol·licitant Entitat Corregida` | `up_solicitant_entitat_corregida` | `TEXT` |
| 21 | `UT sol·licitant desc` | `ut_solicitant_descripcio` | `TEXT` |
| 22 | `Prestació estat codi` | `prestacio_estat_codi` | `TEXT` |
| 23 | `Pacient (NHC)` | `pacient_nhc` | `TEXT` |

### `llee_centre_prova`

Hoja LLEE per Centre i Prova.

- Archivo de referencia: `data/input/test/llee_mensual.xlsx`
- Hoja: `LLEE per Centre i Prova`

| orden | columna_origen | columna_postgresql | tipo_postgresql |
| ---: | --- | --- | --- |
| 1 | `any` | `any_llee` | `TEXT` |
| 2 | `mes` | `mes_llee` | `TEXT` |
| 3 | `Centre` | `centre` | `TEXT` |
| 4 | `Proves` | `proves` | `TEXT` |
| 5 | `Prestacions` | `prestacions` | `TEXT` |
| 6 | `Pendents de programar` | `pendents_programar` | `TEXT` |
| 7 | `% pendents programar` | `percentatge_pendents_programar` | `TEXT` |
| 8 | `Mes de 90 dies` | `mes_90_dies` | `TEXT` |
| 9 | `% mes 90 dies` | `percentatge_mes_90_dies` | `TEXT` |
| 10 | `Entrades` | `entrades` | `TEXT` |
| 11 | `Sortides` | `sortides` | `TEXT` |
| 12 | `Diferència Entrades-Sortides` | `diferencia_entrades_sortides` | `TEXT` |
| 13 | `Temps de demora (dies)` | `temps_demora_dies` | `TEXT` |
| 14 | `Temps d'espera (dies)` | `temps_espera_dies` | `TEXT` |
| 15 | `TD/TE (%)` | `percentatge_td_te` | `TEXT` |

### `llee_trams_demora`

Hoja Trams demora per Centre i Prova.

- Archivo de referencia: `data/input/test/llee_mensual.xlsx`
- Hoja: `Trams demora per Centre i Prova`

| orden | columna_origen | columna_postgresql | tipo_postgresql |
| ---: | --- | --- | --- |
| 1 | `any` | `any_llee` | `TEXT` |
| 2 | `mes` | `mes_llee` | `TEXT` |
| 3 | `Centre` | `centre` | `TEXT` |
| 4 | `Prioritat` | `prioritat` | `TEXT` |
| 5 | `Grup de monitorització` | `grup_monitoritzacio` | `TEXT` |
| 6 | `Total pacients` | `total_pacients` | `TEXT` |
| 7 | `porc_oportunidad` | `percentatge_oportunitat` | `TEXT` |
| 8 | `0-30 d` | `tram_0_30_dies` | `TEXT` |
| 9 | `31-60 d` | `tram_31_60_dies` | `TEXT` |
| 10 | `61-90 d` | `tram_61_90_dies` | `TEXT` |
| 11 | `91-120 d` | `tram_91_120_dies` | `TEXT` |
| 12 | `121-150 d` | `tram_121_150_dies` | `TEXT` |
| 13 | `151-180 d` | `tram_151_180_dies` | `TEXT` |
| 14 | `181-365 d` | `tram_181_365_dies` | `TEXT` |
| 15 | `Mes d'1 any` | `mes_1_any` | `TEXT` |

### `td_ambulatoris`

Hoja td_ambulatoris.

- Archivo de referencia: `data/input/test/td.xlsx`
- Hoja: `td_ambulatoris`

| orden | columna_origen | columna_postgresql | tipo_postgresql |
| ---: | --- | --- | --- |
| 1 | `any` | `any_ambulatoris` | `TEXT` |
| 2 | `mes` | `mes_ambulatoris` | `TEXT` |
| 3 | `centre_realitzacio` | `centre_realitzacio` | `TEXT` |
| 4 | `sala_realitzacio` | `sala_realitzacio` | `TEXT` |
| 5 | `data_realitzacio` | `data_realitzacio` | `TEXT` |
| 6 | `dies_previstos` | `dies_previstos` | `TEXT` |
| 7 | `data_conveni` | `data_conveni` | `TEXT` |
| 8 | `prestacio_modalitat` | `prestacio_modalitat` | `TEXT` |
| 9 | `pte_num` | `pacient_numero` | `TEXT` |
| 10 | `prestacio_denominacio` | `prestacio_denominacio` | `TEXT` |
| 11 | `especialista` | `especialista` | `TEXT` |
| 12 | `situacio` | `situacio` | `TEXT` |
| 13 | `data_resultats` | `data_resultats` | `TEXT` |
| 14 | `pte_cip` | `pacient_cip` | `TEXT` |
| 15 | `data_assignacio` | `data_assignacio` | `TEXT` |
| 16 | `dias_trigats` | `dies_trigats` | `TEXT` |
| 17 | `dias_retard` | `dies_retard` | `TEXT` |

### `td_urgencies`

Hoja td_urgencies.

- Archivo de referencia: `data/input/test/td.xlsx`
- Hoja: `td_urgencies`

| orden | columna_origen | columna_postgresql | tipo_postgresql |
| ---: | --- | --- | --- |
| 1 | `solicitant` | `solicitant` | `TEXT` |
| 2 | `any` | `any_urgencies` | `TEXT` |
| 3 | `mes` | `mes_urgencies` | `TEXT` |
| 4 | `episodi` | `episodi` | `TEXT` |
| 5 | `prestació` | `prestacio` | `TEXT` |
| 6 | `proves` | `proves` | `TEXT` |
| 7 | `lat` | `latitud` | `TEXT` |
| 8 | `lon` | `longitud` | `TEXT` |

## Tabla técnica `load_control`

`load_control` existe en los esquemas `test` y `prd`.

Propósito:

- Registrar trazabilidad de ejecuciones.
- Separar cargas por flujo, entorno y tabla física.
- Registrar hash del archivo fuente.
- Dar soporte a auditoría operativa, reintentos y seguimiento de estado.

No debe crearse con `CREATE TABLE AS`, porque requiere `BIGSERIAL`, `PRIMARY KEY`, `DEFAULT CURRENT_TIMESTAMP` e índice único.

## Reglas para futuras implementaciones

- Cada flujo debe declarar explícitamente su tabla destino.
- El esquema destino se deriva exclusivamente de `--env`.
- Antes de cargar, el pipeline debe validar que las columnas esperadas coincidan con este contrato.
- La carga debe fallar si la tabla destino no existe o si el contrato de columnas no coincide.
- No se debe almacenar la contraseña PostgreSQL en Git ni en archivos versionados.
