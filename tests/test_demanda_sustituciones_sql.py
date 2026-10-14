"""Check SQL contracts statically and prepare synthetic PostgreSQL integration tests.

Static tests need pytest, psycopg and repository configuration. Semantic tests
execute the actual SQL in a disposable schema on an explicitly selected LOCAL
Unix-socket PostgreSQL connection; every change is rolled back. No Aurora access,
existing-table changes or automatic database connections are permitted.
Integration tests are skipped unless UPTOPG_RUN_SQL_TESTS=1 and
UPTOPG_SQL_TEST_DSN explicitly supplies a Unix-socket host and database name.
Example for a later authorized run: UPTOPG_RUN_SQL_TESTS=1
UPTOPG_SQL_TEST_DSN='host=/tmp dbname=uptopg_sql_tests' pytest -q <this file>.
Passing static tests is not evidence that PostgreSQL executed the SQL correctly.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
import pytest

from up_to_postgresql.config.resolver import resolve_flow_config
from up_to_postgresql.flows.demanda_sustituciones_sql import (
    ALGORITHM_VERSION,
    RESULT_COLUMNS,
    SOURCE_COLUMNS,
    build_queries,
)


DDL_PATH = (
    Path(__file__).resolve().parents[1] / "docs/sql/create_demanda_sustituciones.sql"
)


def test_source_columns_match_existing_demanda_mapping() -> None:
    config = resolve_flow_config("demanda", "test")
    assert SOURCE_COLUMNS == tuple(
        item["target"] for item in config.data["load"]["column_mapping"]
    )
    assert len(SOURCE_COLUMNS) == len(set(SOURCE_COLUMNS)) == 23


def test_ddl_and_projection_have_the_same_columns() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    declared = re.findall(
        r"^    ([a-z_0-9]+) (?:text|smallint|bigint|boolean|timestamptz)\b",
        ddl,
        flags=re.MULTILINE,
    )
    assert tuple(declared) == RESULT_COLUMNS
    assert len(RESULT_COLUMNS) == len(set(RESULT_COLUMNS))
    assert ddl.count("CREATE INDEX ") == 1
    assert "CREATE UNIQUE INDEX" not in ddl
    assert 'ON :"schema".demanda_sustituciones (any_prestacio, id_al)' in ddl


def test_schema_is_quoted_and_only_context_requires_parameters() -> None:
    schema = 'a"; --'
    queries = build_queries(schema)
    assert sql.Identifier(schema, "demanda").as_string() in queries["snapshot"]
    assert (
        sql.Identifier(schema, "demanda_sustituciones").as_string()
        in queries["insert_year"]
    )
    assert [name for name, query in queries.items() if "%(" in query] == ["context"]
    assert list(queries).index("validate_source") < list(queries).index("identify")
    validation_steps = [
        name for name in queries
        if name.startswith("validate_result_")
    ]
    assert validation_steps
    assert all(
        list(queries).index(name) < list(queries).index("delete_year")
        for name in validation_steps
    )
    assert list(queries)[-1] == "validate_published"


@pytest.mark.parametrize("schema", [None, "", "a\x00b"])
def test_reject_invalid_schema(schema: str | None) -> None:
    with pytest.raises(ValueError):
        build_queries(schema)


@pytest.fixture
def database():
    if os.environ.get("UPTOPG_RUN_SQL_TESTS") != "1":
        pytest.skip("PostgreSQL real pendiente; requiere habilitación local explícita")
    settings = conninfo_to_dict(os.environ.get("UPTOPG_SQL_TEST_DSN", ""))
    host = settings.get("host", "")
    if (
        not host.startswith("/")
        or "," in host
        or not settings.get("dbname")
        or settings.get("hostaddr")
        or settings.get("service")
        or os.environ.get("PGHOSTADDR")
        or os.environ.get("PGSERVICE")
    ):
        pytest.fail("SQL tests require an explicit local Unix socket and database")
    settings.setdefault("connect_timeout", "3")
    connection = psycopg.connect(**settings)
    try:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SET LOCAL statement_timeout = '15s'")
            schema = "ds_sql_" + uuid4().hex
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cursor.execute(
                sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier(schema, "demanda"),
                    sql.SQL(", ").join(
                        sql.SQL('{} text COLLATE "C"').format(sql.Identifier(name))
                        for name in SOURCE_COLUMNS
                    ),
                )
            )
            ddl = DDL_PATH.read_text(encoding="utf-8")

            parts = ddl.split(':"schema"')

            statement = sql.SQL(parts[0])

            for part in parts[1:]:
                statement += sql.Identifier(schema) + sql.SQL(part)

            cursor.execute(statement)

            yield cursor, schema
    finally:
        connection.rollback()
        connection.close()


def _row(state="AL", **overrides):
    values = dict.fromkeys(SOURCE_COLUMNS, "Descripción")
    values.update(
        any_prestacio="2026",
        mes_prestacio="01",
        data_prestacio="15/01/26",
        numero_prestacions="1",
        pacient_nhc="NHC-001",
        nivell_1_codi="RX",
        up_solicitant_corregida="UP-1",
        prestacio_estat_codi=state,
        prestacio_nivell_9_codi="P9-1",
        centre_codi="C-1",
        centre_sap_codi="H-1",
    )
    values.update(overrides)
    return values


def _load(database, rows):
    cursor, schema = database
    cursor.executemany(
        sql.SQL("INSERT INTO {} VALUES ({})").format(
            sql.Identifier(schema, "demanda"),
            sql.SQL(", ").join(sql.Placeholder() for _ in SOURCE_COLUMNS),
        ),
        [tuple(row[name] for name in SOURCE_COLUMNS) for row in rows],
    )


def _calculate(database, *, year=2026, stop="track_changes"):
    cursor, schema = database
    for name, query in build_queries(schema).items():
        cursor.execute(query, {"anio": year} if name == "context" else None)
        if name == stop:
            break
    cursor.execute("SELECT * FROM pg_temp._ds_result ORDER BY id_relacion")
    return cursor.fetchall()


def _drop_work(cursor):
    for suffix in ("affected", "result", "pairs", "occurrences", "source", "context"):
        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(
                sql.Identifier("pg_temp", "_ds_" + suffix)
            )
        )


def _sha(value):
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def test_postgres_single_candidate_and_content_identifiers(database) -> None:
    original = _row()
    candidate = _row("CF")
    _load(database, [original, candidate])
    records = _calculate(database)
    assert len(records) == 1
    result = records[0]
    assert result["clasificacion"] == "CANDIDATA_UNICA"
    assert result["numero_candidatas"] == 1
    assert result["id_al"] == _sha([[original[name] for name in SOURCE_COLUMNS], 1])
    assert result["id_candidata"] == _sha(
        [
            [candidate[name] for name in SOURCE_COLUMNS],
            1,
        ]
    )
    assert result["id_relacion"] == _sha(
        [
            2026,
            result["id_al"],
            result["id_candidata"],
            ALGORITHM_VERSION,
        ]
    )
    assert result["fecha_carga"].tzinfo is not None


def test_postgres_multiple_candidates(database) -> None:
    _load(database, [_row(), _row("CF"), _row("DI")])
    records = _calculate(database)
    assert len(records) == 2
    assert {r["numero_candidatas"] for r in records} == {2}
    assert {r["clasificacion"] for r in records} == {"CANDIDATAS_MULTIPLES"}


@pytest.mark.parametrize(
    "cardinality, classification",
    [
        ("1", "SIN_CANDIDATA"),
        ("0002", "REGISTRO_AGREGADO"),
        ("9223372036854775807", "REGISTRO_AGREGADO"),
    ],
)
def test_postgres_unmatched_or_aggregated(
    database, cardinality, classification
) -> None:
    _load(database, [_row(numero_prestacions=cardinality)])
    records = _calculate(database)
    assert len(records) == 1
    result = records[0]
    assert result["clasificacion"] == classification
    assert result["numero_prestacions_al"] == int(cardinality)
    assert result["numero_candidatas"] == 0
    assert result["mismo_nivel_9"] is None
    assert all(
        value is None for key, value in result.items() if key.endswith("_candidata")
    )


def test_postgres_aggregates_do_not_match_even_with_equal_keys(database) -> None:
    _load(database, [_row(numero_prestacions="2"), _row("CF")])
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["clasificacion"] == "REGISTRO_AGREGADO"
    assert records[0]["id_candidata"] is None


def test_postgres_aggregated_candidates_are_excluded(database) -> None:
    _load(database, [_row(), _row("CF", numero_prestacions="2")])
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["clasificacion"] == "SIN_CANDIDATA"


def test_postgres_identical_rows_preserve_multiplicity(database) -> None:
    _load(database, [_row(), _row(), _row("CF"), _row("CF")])
    records = _calculate(database)
    assert len(records) == 4
    assert len({r["id_al"] for r in records}) == 2
    assert len({r["id_candidata"] for r in records}) == 2
    assert len({r["id_relacion"] for r in records}) == 4
    assert {r["numero_candidatas"] for r in records} == {2}


def test_postgres_identical_aggregates_preserve_row_multiplicity(database) -> None:
    _load(
        database,
        [_row(numero_prestacions="2"), _row(numero_prestacions="2"), _row("CF")],
    )
    records = _calculate(database)
    assert len(records) == 2
    assert len({r["id_al"] for r in records}) == 2
    assert all(r["clasificacion"] == "REGISTRO_AGREGADO" for r in records)
    assert all(r["numero_prestacions_al"] == 2 for r in records)
    assert all(r["id_candidata"] is None for r in records)


def test_postgres_canonical_content_distinguishes_null_empty_and_literal(
    database,
) -> None:
    _load(
        database,
        [_row(), *(_row("CF", institucio=value) for value in (None, "", "null"))],
    )
    records = _calculate(database)
    assert len(records) == 3
    assert len({r["id_candidata"] for r in records}) == 3


def test_postgres_multiple_al_can_share_one_candidate(database) -> None:
    _load(database, [_row(), _row(prestacio_nivell_9_codi="P9-2"), _row("CF")])
    records = _calculate(database)
    assert len(records) == 2
    assert len({r["id_al"] for r in records}) == 2
    assert len({r["id_candidata"] for r in records}) == 1
    assert {r["numero_candidatas"] for r in records} == {1}


@pytest.mark.parametrize("state", ["CF", "DI", "EC", "RE", "SN", "CO"])
def test_postgres_allowed_states(database, state) -> None:
    _load(database, [_row(), _row(state)])
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["prestacio_estat_codi_candidata"] == state


@pytest.mark.parametrize("state", ["AL", "XX", "cf", "", None])
def test_postgres_excluded_states(database, state) -> None:
    _load(database, [_row(), _row(state)])
    records = _calculate(database)
    assert len(records) == (2 if state == "AL" else 1)
    assert all(r["clasificacion"] == "SIN_CANDIDATA" for r in records)


@pytest.mark.parametrize(
    "original, candidate, expected",
    [
        ("P9-1", "P9-1", True),
        ("P9-1", "P9-2", False),
        (None, "P9-1", None),
        ("P9-1", None, None),
        ("", "", None),
        (" \t", "P9-1", None),
    ],
)
def test_postgres_nivel_9_ternary(database, original, candidate, expected) -> None:
    _load(
        database,
        [
            _row(prestacio_nivell_9_codi=original),
            _row("CF", prestacio_nivell_9_codi=candidate),
        ],
    )
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["mismo_nivel_9"] is expected
    assert records[0]["id_candidata"] is not None


def test_postgres_centres_and_descriptions_survive_matching(database) -> None:
    _load(
        database,
        [
            _row(
                centre_codi="C-A",
                centre_sap_codi="H-A",
                institucio_corregida="I-A",
                prestacio_descripcio="Prestació A",
                prestacio_nivell_9_descripcio="N9 A",
            ),
            _row(
                "CF",
                centre_codi="C-B",
                centre_sap_codi="H-B",
                institucio_corregida="I-B",
                prestacio_descripcio="Prestació B",
                prestacio_nivell_9_descripcio="N9 B",
            ),
        ],
    )
    result = _calculate(database)[0]
    assert result["centre_codi_al"] == "C-A"
    assert result["centre_codi_candidata"] == "C-B"
    assert result["centre_sap_codi_al"] == "H-A"
    assert result["centre_sap_codi_candidata"] == "H-B"
    assert result["institucio_corregida_al"] == "I-A"
    assert result["institucio_corregida_candidata"] == "I-B"
    assert result["prestacio_descripcio_al"] == "Prestació A"
    assert result["prestacio_descripcio_candidata"] == "Prestació B"
    assert result["prestacio_nivell_9_descripcio_al"] == "N9 A"
    assert result["prestacio_nivell_9_descripcio_candidata"] == "N9 B"


@pytest.mark.parametrize(
    "key",
    [
        "pacient_nhc",
        "nivell_1_codi",
        "up_solicitant_corregida",
        "data_prestacio",
    ],
)
@pytest.mark.parametrize("missing", [None, "", " \t\n"])
def test_postgres_missing_keys_never_match(database, key, missing) -> None:
    _load(database, [_row(**{key: missing}), _row("CF", **{key: missing})])
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["clasificacion"] == "SIN_CANDIDATA"
    assert records[0][key] is None


@pytest.mark.parametrize(
    "key, value",
    [
        ("pacient_nhc", "NHC-002"),
        ("nivell_1_codi", "RM"),
        ("up_solicitant_corregida", "UP-2"),
        ("data_prestacio", "2026-01-15"),
        ("data_prestacio", "15/1/26"),
        ("any_prestacio", "2025"),
    ],
)
def test_postgres_each_matching_key_must_be_equal(database, key, value) -> None:
    _load(database, [_row(), _row("CF", **{key: value})])
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["clasificacion"] == "SIN_CANDIDATA"
    assert records[0]["any_prestacio"] == 2026


@pytest.mark.parametrize("missing", [None, "", " \t"])
def test_postgres_candidate_without_year_cannot_match(database, missing) -> None:
    _load(database, [_row(), _row("CF", any_prestacio=missing)])
    records = _calculate(database)
    assert len(records) == 1
    assert records[0]["clasificacion"] == "SIN_CANDIDATA"


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        "",
        "0",
        "000",
        "-1",
        "+1",
        "1.0",
        "1,0",
        "x",
        " 1",
        "1\n",
        "１",
        "9223372036854775808",
        "0009223372036854775808",
        "9" * 100,
    ],
)
def test_postgres_reject_invalid_cardinality_before_cast(database, invalid) -> None:
    _load(database, [_row(), _row("XX", numero_prestacions=invalid)])
    with pytest.raises(psycopg.errors.CheckViolation, match="cardinalidad_invalida"):
        _calculate(database)


def test_postgres_leading_zeros_are_valid(database) -> None:
    _load(database, [_row(numero_prestacions="0" * 100 + "1"), _row("CF")])
    result = _calculate(database)[0]
    assert result["numero_prestacions_al"] == 1
    assert result["clasificacion"] == "CANDIDATA_UNICA"


@pytest.mark.parametrize("year", [None, 0, -2026, 10000, "2026.0"])
def test_postgres_reject_invalid_year(database, year) -> None:
    _load(database, [_row()])
    with pytest.raises(psycopg.errors.CheckViolation, match="anio_invalido"):
        _calculate(database, year=year)


def test_postgres_reject_absent_year(database) -> None:
    _load(database, [_row(any_prestacio="2025")])
    with pytest.raises(psycopg.errors.CheckViolation, match="anio_sin_datos"):
        _calculate(database)


def test_postgres_year_without_al_has_empty_result(database) -> None:
    _load(database, [_row("CF")])
    assert _calculate(database, stop="validate_published") == []


def test_postgres_identifiers_are_stable_after_reordering(database) -> None:
    cursor, _ = database
    rows = [_row(), _row(), _row("CF", institucio='A|B"á'), _row("DI", institucio=None)]
    _load(database, rows)
    cursor.execute("SAVEPOINT calculation")
    first = _calculate(database)
    cursor.execute("ROLLBACK TO SAVEPOINT calculation")
    cursor.execute(
        sql.SQL("DELETE FROM {}").format(sql.Identifier(database[1], "demanda"))
    )
    _load(database, list(reversed(rows)))
    second = _calculate(database)
    assert [dict(r, fecha_carga=None) for r in first] == [
        dict(r, fecha_carga=None) for r in second
    ]


@pytest.mark.parametrize(
    "mutation, expected",
    [
        ("DELETE FROM pg_temp._ds_result", "cobertura_al"),
        (
            "INSERT INTO pg_temp._ds_result SELECT * FROM pg_temp._ds_result",
            "identificadores_relacion",
        ),
        (
            "DELETE FROM pg_temp._ds_result WHERE prestacio_estat_codi_candidata = 'DI'",
            "parejas_completas",
        ),
        (
            "UPDATE pg_temp._ds_result SET id_candidata = repeat('0', 64)",
            "parejas_completas",
        ),
        ("UPDATE pg_temp._ds_result SET numero_candidatas = 99", "filas_por_al"),
        (
            "UPDATE pg_temp._ds_result SET clasificacion = 'SIN_CANDIDATA'",
            "clasificacion_cardinalidad",
        ),
        (
            "UPDATE pg_temp._ds_result SET numero_prestacions_al = 2",
            "clasificacion_cardinalidad",
        ),
        ("UPDATE pg_temp._ds_result SET any_prestacio = 2025", "anio_version_fecha"),
        (
            "UPDATE pg_temp._ds_result SET id_relacion = repeat('0', 64) "
            "WHERE prestacio_estat_codi_candidata = 'CF'",
            "huella_relacion",
        ),
        (
            "UPDATE pg_temp._ds_result SET prestacio_estat_codi_candidata = 'XX'",
            "atributos_fuente",
        ),
        (
            "UPDATE pg_temp._ds_result SET prestacio_descripcio_candidata = 'incorrecta'",
            "atributos_fuente",
        ),
        ("UPDATE pg_temp._ds_result SET mismo_nivel_9 = NULL", "mismo_nivel_9"),
    ],
)
def test_postgres_validation_rejects_corrupted_results(
    database, mutation, expected
) -> None:
    cursor, schema = database
    _load(database, [_row(), _row("CF"), _row("DI")])
    _calculate(database, stop="generate")
    cursor.execute(mutation)
    with pytest.raises(psycopg.errors.CheckViolation, match=expected):
        cursor.execute(build_queries(schema)[f"validate_result_{expected}"])


def test_postgres_validation_rejects_forged_occurrence_identity(database) -> None:
    cursor, schema = database
    _load(database, [_row(), _row("CF")])
    _calculate(database)
    cursor.execute(
        "UPDATE pg_temp._ds_occurrences SET id_registro = repeat('0', 64) "
        "WHERE prestacio_estat_codi = 'AL'"
    )
    with pytest.raises(psycopg.errors.CheckViolation, match="huella_ocurrencia"):
        cursor.execute(build_queries(schema)["validate_identities"])


@pytest.mark.parametrize(
    "assignment",
    [
        "id_relacion = 'invalid'",
        "numero_prestacions_al = 0",
        "numero_candidatas = -1",
        "any_prestacio = 0",
        "clasificacion = 'OTHER'",
        "mismo_nivel_9 = NULL",
        "prestacio_estat_codi_candidata = NULL",
        "pacient_nhc = NULL",
        "data_prestacio = ''",
    ],
)
def test_postgres_ddl_checks_reject_invalid_rows(database, assignment) -> None:
    cursor, schema = database
    _load(database, [_row(), _row("CF")])
    _calculate(database, stop="generate")
    cursor.execute("UPDATE pg_temp._ds_result SET " + assignment)
    with pytest.raises(psycopg.errors.CheckViolation):
        cursor.execute(build_queries(schema)["track_changes"])
        cursor.execute(build_queries(schema)["insert_year"])


@pytest.mark.parametrize("cardinality", ["1", "2"])
def test_postgres_ddl_rejects_candidate_attributes_without_candidate(
    database, cardinality
):
    cursor, schema = database
    _load(database, [_row(numero_prestacions=cardinality)])
    _calculate(database, stop="generate")
    cursor.execute("UPDATE pg_temp._ds_result SET centre_codi_candidata = 'C-2'")
    with pytest.raises(psycopg.errors.CheckViolation):
        cursor.execute(build_queries(schema)["track_changes"])
        cursor.execute(build_queries(schema)["insert_year"])


def test_postgres_year_replacement_and_failed_validation_rollback(database) -> None:
    cursor, schema = database
    _load(database, [_row(any_prestacio="2025"), _row(), _row("CF")])
    _calculate(database, year=2025, stop="validate_published")
    _drop_work(cursor)
    _calculate(database, stop="validate_published")
    _drop_work(cursor)
    target = sql.Identifier(schema, "demanda_sustituciones")
    cursor.execute(sql.SQL("SELECT * FROM {} ORDER BY id_relacion").format(target))
    before = cursor.fetchall()
    cursor.execute("SAVEPOINT replacement")
    _calculate(database, stop="insert_year")
    cursor.execute("UPDATE pg_temp._ds_result SET numero_candidatas = 99")
    with pytest.raises(psycopg.errors.CheckViolation, match="resultado_publicado"):
        cursor.execute(build_queries(schema)["validate_published"])
    cursor.execute("ROLLBACK TO SAVEPOINT replacement")
    cursor.execute(sql.SQL("SELECT * FROM {} ORDER BY id_relacion").format(target))
    assert cursor.fetchall() == before
    after = _calculate(database, stop="validate_published")
    assert len(after) == 1
    assert [dict(r, fecha_carga=None) for r in after] == [
        dict(r, fecha_carga=None) for r in before if r["any_prestacio"] == 2026
    ]
    cursor.execute(
        sql.SQL("SELECT * FROM {} WHERE any_prestacio = 2025").format(target)
    )
    assert cursor.fetchall() == [r for r in before if r["any_prestacio"] == 2025]


def test_postgres_publication_detects_changes_to_other_years(database) -> None:
    cursor, schema = database
    _load(database, [_row()])
    _calculate(database, stop="insert_year")
    cursor.execute("INSERT INTO pg_temp._ds_affected VALUES ('delete', 2025, 1)")
    with pytest.raises(psycopg.errors.CheckViolation, match="otros_anios_modificados"):
        cursor.execute(build_queries(schema)["validate_published"])
