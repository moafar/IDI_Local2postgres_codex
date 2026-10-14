"""SQL for technical demanda substitutions, with occurrence-preserving SHA-256 IDs.

Builds ordered SQL statements; depends on psycopg for identifier quoting and on
PostgreSQL JSONB, SHA-256, temporary tables and PL/pgSQL. It opens no connections
and manages no transactions. The future caller must execute ALL statements in
one analytical transaction, rolling back on any error and committing only after
validate_published. Only context takes parameters: {"anio": an_integer_year}.
The target must already exist; source columns follow demanda's 23-column mapping.
Run once per transaction: temporary tables are removed ON COMMIT DROP.
The year parameter is an integer in 1..9999; source years use four-digit YYYY text.
Calendar authorization, locking, orchestration and reporting belong to later phases.
"""

from __future__ import annotations

from psycopg import sql


ALGORITHM_VERSION = "1.0.0"
CANDIDATE_STATES = ("CF", "DI", "EC", "RE", "SN", "CO")
SOURCE_COLUMNS = (
    "any_prestacio",
    "mes_prestacio",
    "data_prestacio",
    "numero_prestacions",
    "centre_sap_codi",
    "centre_codi",
    "institucio",
    "institucio_corregida",
    "maquina_prestacio_codi",
    "maquina_prestacio_descripcio",
    "maquina_descripcio",
    "prestacio_descripcio",
    "prestacio_nivell_9_codi",
    "prestacio_nivell_9_descripcio",
    "nivell_1_codi",
    "nivell_1_descripcio",
    "up_solicitant_descripcio",
    "up_solicitant_entitat_proveidora_descripcio",
    "up_solicitant_corregida",
    "up_solicitant_entitat_corregida",
    "ut_solicitant_descripcio",
    "prestacio_estat_codi",
    "pacient_nhc",
)
MATCH_COLUMNS = (
    "pacient_nhc",
    "nivell_1_codi",
    "up_solicitant_corregida",
    "data_prestacio",
    "any_prestacio",
)
PAIRED_ATTRIBUTES = (
    "prestacio_nivell_9_codi",
    "prestacio_nivell_9_descripcio",
    "prestacio_descripcio",
    "centre_codi",
    "centre_sap_codi",
    "institucio_corregida",
)
RESULT_COLUMNS = (
    "id_relacion",
    "id_al",
    "id_candidata",
    "any_prestacio",
    "pacient_nhc",
    "nivell_1_codi",
    "up_solicitant_corregida",
    "data_prestacio",
    "numero_prestacions_al",
    "prestacio_estat_codi_candidata",
    *(f"{name}_{side}" for name in PAIRED_ATTRIBUTES for side in ("al", "candidata")),
    "mismo_nivel_9",
    "numero_candidatas",
    "clasificacion",
    "version_algoritmo",
    "fecha_carga",
)


def _present(expression: str) -> str:
    return f"({expression} IS NOT NULL AND {expression} !~ '^[[:space:]]*$')"


def _known(expression: str) -> str:
    return f"CASE WHEN {_present(expression)} THEN {expression} ELSE NULL::text END"


def _hash(expression: str) -> str:
    return f"encode(sha256(convert_to(({expression})::text, 'UTF8')), 'hex')"


def _assertions(checks: dict[str, str]) -> str:
    """Raise check_violation without embedding source values in error messages."""
    body = "\n".join(
        f"IF EXISTS ({query}) THEN\n"
        f"    RAISE EXCEPTION USING ERRCODE = '23514', "
        f"MESSAGE = 'demanda_sustituciones: {name}';\nEND IF;"
        for name, query in checks.items()
    )
    return f"DO $ds$\nBEGIN\n{body}\nEND;\n$ds$;"


def _difference(left: str, right: str) -> str:
    """Bidirectional multiset comparison: duplicates are significant."""
    return f"({left} EXCEPT ALL {right}) UNION ALL ({right} EXCEPT ALL {left})"


def build_queries(schema: str) -> dict[str, str]:
    """Return SQL in execution order; schema is quoted, year is a bound parameter."""
    if not isinstance(schema, str) or not schema or "\x00" in schema:
        raise ValueError("schema must be a non-empty PostgreSQL identifier")
    source = sql.Identifier(schema, "demanda").as_string()
    target = sql.Identifier(schema, "demanda_sustituciones").as_string()
    columns = ", ".join(RESULT_COLUMNS)
    states = ", ".join(f"'{state}'" for state in CANDIDATE_STATES)
    payload = f"jsonb_build_array({', '.join(SOURCE_COLUMNS)})"
    equality = " AND ".join(f"a.{name} = b.{name}" for name in MATCH_COLUMNS)
    keys_present = " AND ".join(_present(f"a.{name}") for name in MATCH_COLUMNS)
    admissible = (
        "a.prestacio_estat_codi = 'AL' AND a.cardinalidad = 1 "
        f"AND b.prestacio_estat_codi IN ({states}) AND b.cardinalidad = 1 "
        f"AND {equality} AND {keys_present}"
    )
    expected_pairs = (
        "SELECT a.id_registro AS id_al, b.id_registro AS id_candidata "
        "FROM pg_temp._ds_occurrences a JOIN pg_temp._ds_occurrences b "
        f"ON {admissible}"
    )
    relation_id = _hash(
        "jsonb_build_array(c.anio_texto::smallint, a.id_registro, "
        "p.id_candidata, c.version_algoritmo)"
    )
    projections = [
        f"{relation_id} AS id_relacion",
        "a.id_registro AS id_al",
        "p.id_candidata",
        "c.anio_texto::smallint AS any_prestacio",
        *(_known(f"a.{name}") + f" AS {name}" for name in MATCH_COLUMNS[:-1]),
        "a.cardinalidad AS numero_prestacions_al",
        "b.prestacio_estat_codi AS prestacio_estat_codi_candidata",
        *(
            _known(f"{alias}.{name}") + f" AS {name}_{side}"
            for name in PAIRED_ATTRIBUTES
            for alias, side in (("a", "al"), ("b", "candidata"))
        ),
        f"CASE WHEN {_present('a.prestacio_nivell_9_codi')} "
        f"AND {_present('b.prestacio_nivell_9_codi')} "
        "THEN a.prestacio_nivell_9_codi = b.prestacio_nivell_9_codi "
        "ELSE NULL::boolean END AS mismo_nivel_9",
        "coalesce(p.numero_candidatas, 0)::bigint AS numero_candidatas",
        "CASE WHEN a.cardinalidad > 1 THEN 'REGISTRO_AGREGADO' "
        "WHEN p.id_candidata IS NULL THEN 'SIN_CANDIDATA' "
        "WHEN p.numero_candidatas = 1 THEN 'CANDIDATA_UNICA' "
        "ELSE 'CANDIDATAS_MULTIPLES' END::text AS clasificacion",
        "c.version_algoritmo",
        "c.fecha_carga",
    ]
    queries = {
        "context": f"""
            CREATE TEMP TABLE _ds_context ON COMMIT DROP AS
            SELECT %(anio)s::text AS anio_texto,
                   '{ALGORITHM_VERSION}'::text AS version_algoritmo,
                   statement_timestamp() AS fecha_carga;
        """,
        "validate_context": _assertions(
            {
                "anio_invalido": "SELECT 1 FROM pg_temp._ds_context "
                "WHERE anio_texto IS NULL OR anio_texto !~ '^[1-9][0-9]{0,3}$'",
            }
        ),
        "snapshot": f"""
            CREATE TEMP TABLE _ds_source ON COMMIT DROP AS
            SELECT {", ".join(f'd.{name} COLLATE "C" AS {name}' for name in SOURCE_COLUMNS)}
            FROM {source} d CROSS JOIN pg_temp._ds_context c
            WHERE d.any_prestacio COLLATE "C" = lpad(c.anio_texto, 4, '0');
        """,
        "validate_source": _assertions(
            {
                "anio_sin_datos": "SELECT 1 WHERE NOT EXISTS "
                "(SELECT 1 FROM pg_temp._ds_source)",
                "cardinalidad_invalida": """
                SELECT 1 FROM pg_temp._ds_source
                WHERE numero_prestacions IS NULL
                   OR numero_prestacions !~ '^[0-9]+$'
                   OR length(ltrim(numero_prestacions, '0')) NOT BETWEEN 1 AND 19
                   OR (length(ltrim(numero_prestacions, '0')) = 19
                       AND ltrim(numero_prestacions, '0') COLLATE "C"
                           > '9223372036854775807')
            """,
            }
        ),
        "identify": f"""
            CREATE TEMP TABLE _ds_occurrences ON COMMIT DROP AS
            WITH grouped AS (
                SELECT {payload} AS contenido, count(*) AS repeticiones
                FROM pg_temp._ds_source GROUP BY {payload}
            )
            SELECT {_hash("jsonb_build_array(g.contenido, n.ordinal)")} COLLATE "C"
                       AS id_registro,
                   g.contenido, n.ordinal,
                   {", ".join(f'(g.contenido ->> {i}) COLLATE "C" AS {name}' for i, name in enumerate(SOURCE_COLUMNS))},
                   ltrim(g.contenido ->> 3, '0')::bigint AS cardinalidad
            FROM grouped g CROSS JOIN LATERAL
                 generate_series(1::bigint, g.repeticiones) AS n(ordinal);
        """,
        "validate_identities": _assertions(
            {
                "identificadores_ocurrencia": "SELECT id_registro "
                "FROM pg_temp._ds_occurrences GROUP BY id_registro HAVING count(*) <> 1",
                "multiplicidad_fuente": _difference(
                    "SELECT contenido FROM pg_temp._ds_occurrences",
                    f"SELECT {payload} FROM pg_temp._ds_source",
                ),
                "huella_ocurrencia": "SELECT 1 FROM pg_temp._ds_occurrences "
                f"WHERE id_registro IS DISTINCT FROM {_hash('jsonb_build_array(contenido, ordinal)')} "
                f"OR contenido IS DISTINCT FROM {payload}",
                "ordinal_ocurrencia": "SELECT 1 FROM (SELECT ordinal, "
                "count(*) OVER (PARTITION BY contenido) AS repeticiones "
                "FROM pg_temp._ds_occurrences) numbered "
                "WHERE ordinal IS NULL OR ordinal < 1 OR ordinal > repeticiones",
            }
        ),
        "index_occurrences": """
            CREATE UNIQUE INDEX _ds_occurrences_id_registro_idx
            ON pg_temp._ds_occurrences (id_registro);
        """,
        "analyze_occurrences": """
            ANALYZE pg_temp._ds_occurrences;
        """,
        "match": f"""
            CREATE TEMP TABLE _ds_pairs ON COMMIT DROP AS
            SELECT id_al, id_candidata, count(*) OVER (PARTITION BY id_al)
                   AS numero_candidatas
            FROM ({expected_pairs}) pairs;
        """,
        "generate": f"""
            CREATE TEMP TABLE _ds_result ON COMMIT DROP AS
            SELECT {", ".join(projections)}
            FROM pg_temp._ds_occurrences a CROSS JOIN pg_temp._ds_context c
            LEFT JOIN pg_temp._ds_pairs p ON p.id_al = a.id_registro
            LEFT JOIN pg_temp._ds_occurrences b ON b.id_registro = p.id_candidata
            WHERE a.prestacio_estat_codi = 'AL';
        """,
        "analyze_result": """
            ANALYZE pg_temp._ds_result;
        """,
    }
    checks = {
        "cobertura_al": _difference(
            "SELECT DISTINCT id_al FROM pg_temp._ds_result",
            "SELECT id_registro FROM pg_temp._ds_occurrences "
            "WHERE prestacio_estat_codi = 'AL'",
        ),
        "identificadores_relacion": "SELECT id_relacion FROM pg_temp._ds_result "
        "GROUP BY id_relacion HAVING id_relacion IS NULL OR count(*) <> 1",
        "parejas_completas": _difference(
            "SELECT id_al, id_candidata FROM pg_temp._ds_result "
            "WHERE id_candidata IS NOT NULL",
            expected_pairs,
        ),
        "parejas_admisibles": f"""
            SELECT 1 FROM pg_temp._ds_result r
            LEFT JOIN pg_temp._ds_occurrences a ON a.id_registro = r.id_al
            LEFT JOIN pg_temp._ds_occurrences b ON b.id_registro = r.id_candidata
            WHERE r.id_candidata IS NOT NULL AND ({admissible}) IS NOT TRUE
        """,
        "filas_por_al": f"""
            WITH expected AS ({expected_pairs}), counts AS (
                SELECT a.id_registro,
                       count(e.id_candidata) AS candidatas
                FROM pg_temp._ds_occurrences a
                LEFT JOIN expected e ON e.id_al = a.id_registro
                WHERE a.prestacio_estat_codi = 'AL' GROUP BY a.id_registro
            )
            SELECT 1 FROM counts c
            LEFT JOIN pg_temp._ds_result r ON r.id_al = c.id_registro
            GROUP BY c.id_registro, c.candidatas
            HAVING count(r.id_al) <> greatest(c.candidatas, 1)
               OR count(r.id_candidata) <> c.candidatas
               OR bool_or(r.numero_candidatas IS DISTINCT FROM c.candidatas)
        """,
        "clasificacion_cardinalidad": """
            SELECT 1 FROM pg_temp._ds_result r
            JOIN pg_temp._ds_occurrences a ON a.id_registro = r.id_al
            WHERE r.numero_prestacions_al IS DISTINCT FROM a.cardinalidad
               OR r.clasificacion IS DISTINCT FROM CASE
                    WHEN a.cardinalidad > 1 THEN 'REGISTRO_AGREGADO'
                    WHEN r.numero_candidatas = 0 THEN 'SIN_CANDIDATA'
                    WHEN r.numero_candidatas = 1 THEN 'CANDIDATA_UNICA'
                    ELSE 'CANDIDATAS_MULTIPLES' END
        """,
        "anio_version_fecha": """
            SELECT 1 FROM pg_temp._ds_result r
            JOIN pg_temp._ds_occurrences a ON a.id_registro = r.id_al
            CROSS JOIN pg_temp._ds_context c
            WHERE r.any_prestacio IS DISTINCT FROM c.anio_texto::smallint
               OR r.any_prestacio IS DISTINCT FROM a.any_prestacio::smallint
               OR r.version_algoritmo IS DISTINCT FROM c.version_algoritmo
               OR r.fecha_carga IS DISTINCT FROM c.fecha_carga
        """,
        "huella_relacion": f"""
            SELECT 1 FROM pg_temp._ds_result r
            WHERE r.id_relacion IS DISTINCT FROM {_hash("jsonb_build_array(r.any_prestacio, r.id_al, r.id_candidata, r.version_algoritmo)")}
        """,
    }
    attribute_checks = [
        f"r.{name} IS DISTINCT FROM {_known(f'a.{name}')}"
        for name in MATCH_COLUMNS[:-1]
    ]
    attribute_checks.append(
        "r.prestacio_estat_codi_candidata IS DISTINCT FROM b.prestacio_estat_codi"
    )
    attribute_checks.extend(
        f"r.{name}_{side} IS DISTINCT FROM {_known(f'{alias}.{name}')}"
        for name in PAIRED_ATTRIBUTES
        for alias, side in (("a", "al"), ("b", "candidata"))
    )
    checks["atributos_fuente"] = f"""
        SELECT 1 FROM pg_temp._ds_result r
        JOIN pg_temp._ds_occurrences a ON a.id_registro = r.id_al
        LEFT JOIN pg_temp._ds_occurrences b ON b.id_registro = r.id_candidata
        WHERE {" OR ".join(attribute_checks)}
    """
    checks["mismo_nivel_9"] = f"""
        SELECT 1 FROM pg_temp._ds_result r
        WHERE r.mismo_nivel_9 IS DISTINCT FROM CASE
            WHEN r.id_candidata IS NOT NULL
                 AND {_present("r.prestacio_nivell_9_codi_al")}
                 AND {_present("r.prestacio_nivell_9_codi_candidata")}
            THEN r.prestacio_nivell_9_codi_al = r.prestacio_nivell_9_codi_candidata
            ELSE NULL::boolean END
    """
    for name, check_sql in checks.items():
        queries[f"validate_result_{name}"] = _assertions({name: check_sql})
    queries["track_changes"] = """
        CREATE TEMP TABLE _ds_affected (
            operacion text NOT NULL, any_prestacio smallint NOT NULL, filas bigint NOT NULL
        ) ON COMMIT DROP;
    """
    queries["delete_year"] = f"""
        WITH deleted AS (
            DELETE FROM {target}
            WHERE any_prestacio = (SELECT anio_texto::smallint FROM pg_temp._ds_context)
            RETURNING any_prestacio
        )
        INSERT INTO pg_temp._ds_affected
        SELECT 'delete', any_prestacio, count(*) FROM deleted GROUP BY any_prestacio;
    """
    queries["insert_year"] = f"""
        WITH inserted AS (
            INSERT INTO {target} ({columns})
            SELECT {columns} FROM pg_temp._ds_result
            RETURNING any_prestacio
        )
        INSERT INTO pg_temp._ds_affected
        SELECT 'insert', any_prestacio, count(*) FROM inserted GROUP BY any_prestacio;
    """
    queries["validate_published"] = _assertions(
        {
            "otros_anios_modificados": "SELECT 1 FROM pg_temp._ds_affected "
            "WHERE any_prestacio <> (SELECT anio_texto::smallint FROM pg_temp._ds_context)",
            "resultado_publicado": _difference(
                f"SELECT {columns} FROM {target} WHERE any_prestacio = "
                "(SELECT anio_texto::smallint FROM pg_temp._ds_context)",
                f"SELECT {columns} FROM pg_temp._ds_result",
            ),
        }
    )
    return queries
