# Diseño de prestaciones pendientes de IDI

El flujo sustituirá el cálculo mensual en Excel a partir de SAP BO y publicará detalle por prestación para Power BI. Cada descarga de `prestacions_pendents.csv` reemplazará el corte vigente con `load_mode: replace`. La primera carga será en `test`. El período de informe y la fecha de corte son metadatos explícitos del corte. El período y la fecha de prestación son atributos de cada fila.

Diagnóstico realizado en `feature/prestacions-pendents`, commit `a5b7b16`, mediante lectura del código y procesamiento local de `data/input/test/prestacions_pendents.csv`. No se consultó PostgreSQL.

## Controles reproducidos

Período de informe `2026-10` y fecha de corte `2026-11-09`.

| Detalle | Filas | Suma del recuento | Filas de 2026-10 | Suma del recuento de 2026-10 |
| --- | ---: | ---: | ---: | ---: |
| Leído antes del filtro | 252.901 | 252.901 | 27.918 | 27.918 |
| Intervencionista antes del filtro | 911 | 911 | 118 | 118 |
| Excluido | 108 | 108 | 3 | 3 |
| Incluido | 252.793 | 252.793 | 27.915 | 27.915 |

- En todo el detalle, la clave `Centre Codi` + `Prestació número` da **252.901 claves distintas**, **0 filas con algún componente vacío** y **0 filas duplicadas**, contando todos los miembros de cada grupo repetido.
- Los 252.901 registros de detalle tienen 25 campos. Hay 143 regiones sanitarias vacías, 7.819 UT solicitantes vacías, 154.039 códigos de máquina vacíos y 154.204 descripciones de máquina vacías.
- Todas las fechas de prestación tienen formato `DD/MM/AA` y son fechas válidas. Hay 0 fechas vacías y 0 discrepancias entre fecha, período y año de prestación. Los años van de 2021 a 2030. Las 89.164 filas incluidas con fecha de prestación posterior al corte contribuyen a `Acumulat`.
- El recuento es 1 en todas las filas de esta muestra. La lógica calcula las sumas a partir del recuento de cada fila.

## Lectura y configuración

La cabecera está en la línea física 7 y en la segunda fila no vacía, después de `Detall`. `BaseTabularReader.header_row` convierte la posición de base uno al índice de pandas. Al ejecutar el lector real sobre la muestra, `header_row: 2` produjo 25 columnas y 252.901 filas. Con `header_row: 1`, pandas tomó el título como cabecera y produjo una columna con un índice de 24 niveles. `CsvReader.read` ya admite el separador configurado y las comillas dobles por defecto, omite líneas en blanco y conserva los códigos como texto y los vacíos explícitos como cadenas vacías. La lectura de la muestra queda resuelta por configuración. [BaseTabularReader.header_row](../src/up_to_postgresql/readers/base.py#L58) · [CsvReader.read](../src/up_to_postgresql/readers/csv_reader.py#L11)

`FlowRegistry.discover` ya recoge los archivos `.yml` cuyo nombre coincide con `name`. `resolve_flow_config` combina las capas en el orden `common → entorno → flujo` y llama a `validate_flow_config`. Se verificó la resolución de `demanda` en los dos entornos de la CLI, **`test` y `prd`**. La CLI exige `--execute` para `--load`. Basta añadir el YAML en la siguiente fase. Los metadatos se actualizarán explícitamente en su bloque de transformación con cada descarga mensual. [FlowRegistry.discover](../src/up_to_postgresql/registry.py#L16) · [resolve_flow_config](../src/up_to_postgresql/config/resolver.py#L20) · [build_parser y main](../src/up_to_postgresql/cli.py#L14)

## YAML y contrato físico

Los primeros 25 pares del mapping enumeran el encabezado real en orden. Los seis últimos son derivados. Las 31 columnas físicas de PostgreSQL se almacenan como `TEXT`. El tipado analítico se aplica en Power BI. La tabla física `test.prestacions_pendents` deberá existir antes de la primera carga. El repositorio documenta el contrato de aprovisionamiento de tablas preexistentes y carece de una definición física para esta tabla. El cargador exige igualdad entre la lista de destinos y las columnas físicas en `ordinal_position`. Se conservan los nombres de `demanda` cuando coinciden los campos de origen. [Mapeo de demanda](../config/flows/demanda.yml) · [Contrato de tablas preexistentes](postgresql_mapping.md#reglas-generales) · [_validate_database_contract](../src/up_to_postgresql/loading/postgresql.py#L313) · [_table_columns](../src/up_to_postgresql/loading/postgresql.py#L568)

```yaml
name: prestacions_pendents
source:
  type: csv
  path: prestacions_pendents.csv
  encoding: utf-8
  delimiter: ";"
  header_row: 2
processing:
  drop_empty_rows: false
  drop_empty_columns: false
  trim_strings: false
validation:
  required_columns:
    - Centre Codi
    - Prestació número
  missing_columns_policy: error
  duplicate_key:
    - Centre Codi
    - Prestació número
  duplicate_policy: error
transformations:
  - name: prestacions_pendents
    periode_informe: "2026-10"
    data_tall: "2026-11-09"
    intervencionistes: "Procediments intervencionistes"
    nivell_9_fragments:
      - "guia"
      - "Tècnica de ROLL"
      - "Radiofreqüència"
    nivell_9_exactes:
      - "Marcadores metálicos"
      - "Localización pre-quirúrgica"
      - "Marcadors post BAG"
      - "PAAF d’adenopaties cervicals (a cegues)"
      - "PAAF de parts toves (a cegues)"
      - "PAAF adenopatía axil·lar"
      - "TC osteosíntesi percutània"
    centres:
      AR: IDI Lleida
      BE: IDI Metropolitana Sud
      DT: IDI Girona
      GT: IDI Metropolitana Nord
      JX: IDI Tarragona
      MA: IDI Barcelona
      VC: "IDI Terres de l'Ebre"
      VH: "IDI Vall d'Hebron"
output:
  processed_filename: prestacions_pendents_processed.csv
  report_filename: prestacions_pendents_report.json
load:
  target_table: prestacions_pendents
  load_mode: replace
  reload_existing_hash: false
  column_mapping:
    - source: Any prestació (YYYY)
      target: any_prestacio  # TEXT
    - source: Periode prestació (YYYY-MM)
      target: periode_prestacio  # TEXT
    - source: Data prestació
      target: data_prestacio  # TEXT, DD/MM/AA
    - source: Centre Codi
      target: centre_codi  # TEXT
    - source: Institució corregida
      target: institucio_corregida  # TEXT
    - source: Prestació descripció
      target: prestacio_descripcio  # TEXT
    - source: Prestació nivell 9 desc
      target: prestacio_nivell_9_descripcio  # TEXT
    - source: Pacient (NHC)
      target: pacient_nhc  # TEXT
    - source: Prestació número
      target: prestacio_numero  # TEXT
    - source: Prestació estat codi
      target: prestacio_estat_codi  # TEXT
    - source: Prestació estat codi corregida
      target: prestacio_estat_codi_corregida  # TEXT
    - source: Diagnòstic o Seguiment
      target: diagnostic_o_seguiment  # TEXT
    - source: Descripció Diagnòstic
      target: descripcio_diagnostic  # TEXT
    - source: Prestació prioritat codi
      target: prestacio_prioritat_codi  # TEXT
    - source: UP Sol·licitant Entitat Corregida
      target: up_solicitant_entitat_corregida  # TEXT
    - source: Agrupació sol·licitant
      target: agrupacio_solicitant  # TEXT
    - source: Sol.licitant (agrupació per proveïdor)
      target: solicitant_agrupacio_per_proveidor  # TEXT
    - source: Regió sanitària desc
      target: regio_sanitaria_descripcio  # TEXT
    - source: UP Sol·licitant Corregida
      target: up_solicitant_corregida  # TEXT
    - source: UT sol·licitant desc
      target: ut_solicitant_descripcio  # TEXT
    - source: UT Gestora codi
      target: ut_gestora_codi  # TEXT
    - source: UT Gestora desc
      target: ut_gestora_descripcio  # TEXT
    - source: Màquina de la prestació codi
      target: maquina_prestacio_codi  # TEXT
    - source: Màquina de la prestació desc
      target: maquina_prestacio_descripcio  # TEXT
    - source: Número de prestacions pendents de realitzar
      target: numero_prestacions_pendents  # TEXT
    - source: Centre nom
      target: centre_nom  # TEXT
    - source: Període informe
      target: periode_informe  # TEXT, YYYY-MM
    - source: Data tall
      target: data_tall  # TEXT, entrada ISO YYYY-MM-DD
    - source: Mes prestació número
      target: mes_prestacio_numero  # TEXT, 1–12
    - source: Mes
      target: mes  # TEXT
    - source: Acumulat
      target: acumulat  # TEXT
```

Los códigos, identificadores, períodos y dimensiones vacías se conservan como texto. La fecha de prestación mantiene el formato `DD/MM/AA` y la fecha de corte se almacena como texto ISO `YYYY-MM-DD`. El pipeline valida las fechas y los recuentos, y calcula el número de mes, `Mes` y `Acumulat` antes de cargar. El cargador convierte los valores a texto para su inserción. [_prepare_frame](../src/up_to_postgresql/loading/postgresql.py#L304) · [_insert_rows](../src/up_to_postgresql/loading/postgresql.py#L477)

## Integración en el runner

1. **Validación de configuración.** Añadir una validación específica del nombre `prestacions_pendents` en `_validate_transformations` para el período, la fecha ISO real, las listas de patrones y el mapa de centros. Reflejarla en la definición de transformación del esquema JSON. El parser por subconjunto ya admite la sintaxis del borrador. El registro, la lectura y la resolución de rutas usan los puntos de entrada existentes. [_validate_transformations](../src/up_to_postgresql/config/schema.py#L147) · [_YamlSubsetParser](../src/up_to_postgresql/config/resolver.py#L54) · [Esquema JSON](../config/schemas/flow.schema.json)
2. **Validación previa al filtro.** En la rama actual, `run_flow` lee, limpia, transforma y proyecta antes de validar columnas y duplicados. Después escribe el CSV, ejecuta la carga opcional y escribe el reporte. Para este flujo, contrastar el encabezado con los orígenes del mapping descontando las seis salidas derivadas y validar que los registros tengan la forma esperada. Usar los mecanismos de columnas requeridas y política de duplicados sobre el detalle completo, contando nulos, cadenas vacías y valores de solo espacios como componentes vacíos, sin modificar los identificadores. Rechazar claves duplicadas en cada corte y conservar el recuento de todos los miembros del grupo. Validar también el período de prestación y su coherencia con el año, el recuento como entero no negativo dentro del rango del destino y las fechas informadas en formato `DD/MM/AA` usando el año completo de prestación para validar el calendario. Mantener la comprobación posterior a la proyección y el orden actual de los demás flujos. [run_flow](../src/up_to_postgresql/flows/runner.py#L59) · [_project_mapped_columns](../src/up_to_postgresql/flows/runner.py#L245) · [Validaciones del runner](../src/up_to_postgresql/flows/runner.py#L329)
3. **Transformación.** El runner ejecuta la transformación específica sobre el detalle original y aplica después la proyección común. La categoría intervencionista se compara por igualdad literal y distingue mayúsculas. Para esa categoría, aplicar `casefold()` al nivel 9 y a los patrones del YAML y comprobar fragmentos literales o pertenencia exacta. Comparar el nivel ausente como `''`. Los acentos, apóstrofos y valores originales se preservan. El nombre de centro usa el mapa, con el código literal como etiqueta si es desconocido y nulo si el código es nulo. Una fila con código nulo ya habría incumplido la validación de clave. Derivar el mes entero del período de prestación. `Mes` toma el recuento cuando ambos períodos coinciden y cero en el resto. `Acumulat` toma el recuento de cada fila incluida, de cualquier año y también para prestaciones futuras. [transformación específica](../src/up_to_postgresql/transforms/prestacions_pendents.py)
4. **Protección y reporte.** Abortar antes de escribir el CSV procesado o llamar al cargador ante un archivo ilegible o mal formado, encabezado incorrecto, clave incompleta o duplicada, valores inválidos, detalle vacío o cero filas incluidas. El reporte actual se escribe al final y una política `error` lanza antes de llegar a ese punto. Para este flujo, escribir evidencia de los intentos fallidos usando el reporte JSON, con etapa, motivo, metadatos, hash cuando esté disponible y agregados disponibles. Mantener separado el reporte del último corte publicado del reporte del intento fallido y de la ejecución sin carga. Las evidencias contienen encabezados y agregados, sin valores de claves ni registros. Distinguir filas leídas, excluidas, incluidas y publicadas. Contar las publicadas tras el commit. `load_control.rows_read` conserva su significado actual de filas entregadas al cargador, ya filtradas. [Salida y carga en run_flow](../src/up_to_postgresql/flows/runner.py#L134) · [_report](../src/up_to_postgresql/flows/runner.py#L417) · [PostgresqlLoader.load](../src/up_to_postgresql/loading/postgresql.py#L119)

## Reemplazo y reintentos

El cargador ejecuta el borrado completo, la inserción y el registro de éxito en una transacción. El rollback restaura el corte anterior si falla la carga. Para la operación mensual, ejecutar una carga a la vez por entorno y mantener el fichero estable desde la lectura hasta el final de la carga. El cargador vuelve a abrir la ruta para calcular el hash. [PostgresqlLoader.load](../src/up_to_postgresql/loading/postgresql.py#L119) · [_apply_load_mode](../src/up_to_postgresql/loading/postgresql.py#L406)

Mantener `reload_existing_hash: false` de forma habitual. Para corregir el período o la fecha de corte con un fichero de bytes idénticos, usar `reload_existing_hash: true` en esa ejecución explícita con los metadatos corregidos. Se vuelve a calcular `Mes` y se reemplaza el corte completo. El SHA-256 sigue representando el CSV y el reporte lo vincula con los metadatos aplicados.

La implementación conserva `load_control` y la lógica existente de hashes, conflictos y reintentos. Con override, un reintento fallido puede actualizar el registro de control previo aunque el rollback conserve el corte. El reporte local de error deja evidencia agregada de ese intento. [_validate_source_hash](../src/up_to_postgresql/loading/postgresql.py#L361) · [_insert_load_control](../src/up_to_postgresql/loading/postgresql.py#L499)

El runner escribe `prestacions_pendents_report_preview.json` al procesar sin carga, con cero filas publicadas. Los fallos se registran en `prestacions_pendents_report_error.json`. El reporte `prestacions_pendents_report.json` corresponde a la última publicación exitosa.

## Power BI

La base mantiene estables el año de prestación, el número de mes, `Mes` y `Acumulat`. La matriz requiere sumas de `Mes`, `Acumulat` y recuentos de prestaciones por año. No se encontró un PBIP ni archivos TMDL en el árbol local. Queda pendiente decidir dónde se exponen las columnas anuales al revisar la consulta de origen, el parámetro `EsquemaEntorno` y la matriz. Se recomienda usar el año de prestación como eje de columnas de Power BI y medidas de suma sobre el detalle, con `EsquemaEntorno` seleccionando el esquema. El gráfico puede ordenar por el número entero de mes.

## Pruebas mínimas de la siguiente fase

- Resolver el nuevo YAML por la CLI, comprobar la cabecera y el orden del mapping y ejecutar las pruebas existentes de configuración, lector y runner con `demanda`.
- Reproducir la conciliación de la muestra. Cubrir fragmentos, excepciones exactas, mayúsculas, acentos y apóstrofos, nivel 9 vacío, centros conocidos y desconocidos, nulo en la derivación y prestaciones futuras. Usar un recuento mayor que uno para verificar las sumas.
- Probar una clave duplicada en una fila que habría sido excluida, componentes vacíos, cabecera incorrecta, registro truncado, entrada sin detalle y resultado totalmente excluido. Verificar que se conserva evidencia y que el cargador queda sin invocar.
- En TEST, verificar tabla física preexistente, orden, tipos, ceros iniciales y dimensiones vacías. Reemplazar un corte anterior y forzar un fallo de inserción para verificar rollback. Cubrir el bloqueo por mismo hash y verificar el comportamiento existente al corregir metadatos o reintentar un fallo.

## Verificación de la implementación

El runner reprodujo todos los controles de la muestra en una ejecución sin carga. El reporte registró 252.793 filas incluidas y 0 publicadas. La selección de pruebas del nuevo flujo, configuración, lectores y cargador simulado pasó 61 casos. La regresión focalizada de `demanda` pasó 3 casos. La suite completa registró 92 casos correctos y los mismos 24 fallos que se reprodujeron en `a5b7b16`.

Queda pendiente aprovisionar la tabla en TEST conforme al contrato de esta página y verificar la primera carga contra el catálogo y los tipos reales de PostgreSQL.
