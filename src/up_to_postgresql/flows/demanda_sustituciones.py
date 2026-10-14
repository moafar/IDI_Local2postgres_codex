"""Execute demanda substitution SQL in one independent PostgreSQL transaction.

Depends on the existing flow configuration, loader connection conventions and
demanda_sustituciones_sql. Requires a pre-created result table; it performs no
DDL, calendar authorization, locking, pipeline reporting or other ETL changes.
Close errors after COMMIT retain the original exception and mark that COMMIT
was confirmed, so pipeline reports cannot imply an analytical rollback.
Displays SQL-phase progress and timings; percentages measure completed phases,
not progress within PostgreSQL statements.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Any

from tqdm import tqdm

from up_to_postgresql.config.schema import FlowConfig, validate_flow_config
from up_to_postgresql.flows.demanda_sustituciones_sql import (
    ALGORITHM_VERSION,
    RESULT_COLUMNS,
    build_queries,
)
from up_to_postgresql.loading.postgresql import (
    _default_connection_factory,
    _default_password_provider,
    _postgresql_config,
)


class DemandaSustitucionesError(ValueError):
    """The requested run or the pre-existing result table violates its contract."""


@dataclass(frozen=True)
class DemandaSustitucionesResult:
    anio_procesado: int
    version_algoritmo: str
    registros_al: int
    candidatas_unicas: int
    candidatas_multiples: int
    sin_candidata: int
    registros_agregados: int
    relaciones_generadas: int
    duracion_segundos: float
    estado: str


_NOT_NULL = frozenset(
    {
        "id_relacion",
        "id_al",
        "any_prestacio",
        "numero_prestacions_al",
        "numero_candidatas",
        "clasificacion",
        "version_algoritmo",
        "fecha_carga",
    }
)
_COLUMN_TYPES = {
    "any_prestacio": "int2",
    "numero_prestacions_al": "int8",
    "mismo_nivel_9": "bool",
    "numero_candidatas": "int8",
    "fecha_carga": "timestamptz",
}
_CONSTRAINTS = {
    ("demanda_sustituciones_pkey", "p"),
    ("ds_identificadores", "c"),
    ("ds_dominio", "c"),
    ("ds_clasificacion", "c"),
    ("ds_candidata", "c"),
    ("ds_nivel_9", "c"),
}


def _validate_target(cursor: Any, schema: str) -> None:
    cursor.execute(
        """SELECT column_name, udt_name, is_nullable, collation_name
           FROM information_schema.columns
           WHERE table_schema = %s AND table_name = %s
           ORDER BY ordinal_position""",
        (schema, "demanda_sustituciones"),
    )
    actual = [tuple(row) for row in cursor.fetchall()]
    expected = [
        (
            name,
            _COLUMN_TYPES.get(name, "text"),
            "NO" if name in _NOT_NULL else "YES",
            "C" if name not in _COLUMN_TYPES else None,
        )
        for name in RESULT_COLUMNS
    ]
    if actual != expected:
        raise DemandaSustitucionesError(
            "demanda_sustituciones does not match the expected column contract"
        )

    cursor.execute(
        """SELECT constraint_name, constraint_type
           FROM information_schema.table_constraints
           WHERE table_schema = %s AND table_name = %s""",
        (schema, "demanda_sustituciones"),
    )
    # information_schema uses portable names; map them to pg_constraint kinds.
    kinds = {"PRIMARY KEY": "p", "CHECK": "c"}
    actual_constraints = {
        (name, kinds[kind])
        for name, kind in cursor.fetchall()
        if kind in kinds
    }
    if not _CONSTRAINTS <= actual_constraints:
        raise DemandaSustitucionesError(
            "demanda_sustituciones is missing required constraints"
        )
    cursor.execute(
        """SELECT column_name FROM information_schema.key_column_usage
           WHERE table_schema = %s AND table_name = %s
             AND constraint_name = %s
           ORDER BY ordinal_position""",
        (schema, "demanda_sustituciones", "demanda_sustituciones_pkey"),
    )
    if [row[0] for row in cursor.fetchall()] != ["id_relacion"]:
        raise DemandaSustitucionesError(
            "demanda_sustituciones has an unexpected primary key"
        )


_SUMMARY_SQL = """SELECT
    count(DISTINCT id_al),
    count(DISTINCT id_al) FILTER (WHERE clasificacion = 'CANDIDATA_UNICA'),
    count(DISTINCT id_al) FILTER (WHERE clasificacion = 'CANDIDATAS_MULTIPLES'),
    count(DISTINCT id_al) FILTER (WHERE clasificacion = 'SIN_CANDIDATA'),
    count(DISTINCT id_al) FILTER (WHERE clasificacion = 'REGISTRO_AGREGADO'),
    count(*) FILTER (WHERE id_candidata IS NOT NULL)
    FROM pg_temp._ds_result"""


def generar_sustituciones(
    config: FlowConfig,
    *,
    anio: int,
    connection_factory: Any = None,
    password: str | None = None,
    password_provider: Any = None,
) -> DemandaSustitucionesResult:
    """Replace one year's results after all SQL checks, or roll back the run."""
    if not isinstance(anio, int) or isinstance(anio, bool) or not 1 <= anio <= 9999:
        raise DemandaSustitucionesError("anio must be an integer from 1 to 9999")
    if not isinstance(config, FlowConfig):
        raise DemandaSustitucionesError("config must be a resolved FlowConfig")
    validate_flow_config(config.data, expected_name="demanda", env=config.env)
    if config.name != "demanda":
        raise DemandaSustitucionesError("only the demanda flow is supported")
    postgresql = _postgresql_config(config)
    queries = build_queries(postgresql["schema"])
    if password is None:
        password = (password_provider or _default_password_provider)()

    started = perf_counter()
    connection = (connection_factory or _default_connection_factory)(
        host=postgresql["host"],
        port=postgresql["port"],
        dbname=postgresql["database"],
        user=postgresql["user"],
        password=password,
    )
    try:
        if connection.autocommit:
            raise DemandaSustitucionesError("the connection must be transactional")
        with connection.cursor() as cursor:
            _validate_target(cursor, postgresql["schema"])
            with tqdm(
                total=len(queries), desc=f"Sustituciones {anio}",
                unit="fase", dynamic_ncols=True,
            ) as progress:
                for name, query in queries.items():
                    progress.set_postfix_str(name, refresh=True)
                    phase_started = perf_counter()
                    try:
                        cursor.execute(
                            query, {"anio": anio} if name == "context" else None
                        )
                    except BaseException:
                        tqdm.write(
                            f"[demanda_sustituciones] {name}: ERROR "
                            f"tras {perf_counter() - phase_started:.2f} s"
                        )
                        raise
                    tqdm.write(
                        f"[demanda_sustituciones] {name}: "
                        f"{perf_counter() - phase_started:.2f} s"
                    )
                    progress.update(1)
            cursor.execute(_SUMMARY_SQL)
            counts = cursor.fetchone()
            if counts is None or len(counts) != 6:
                raise DemandaSustitucionesError("missing analytical summary")
            registros_al, unicas, multiples, sin_candidata, agregados, relaciones = (
                int(value) for value in counts
            )
            if registros_al != unicas + multiples + sin_candidata + agregados:
                raise DemandaSustitucionesError("inconsistent AL classification counts")
        connection.commit()
    except BaseException as error:
        try:
            connection.rollback()
        except Exception as rollback_error:
            error.add_note(f"Rollback failed: {type(rollback_error).__name__}")
        try:
            connection.close()
        except Exception as close_error:
            error.add_note(f"Close failed: {type(close_error).__name__}")
        raise
    try:
        connection.close()
    except BaseException as error:
        error.demanda_sustituciones_commit_confirmed = True
        error.add_note("Analytical COMMIT succeeded; connection close failed")
        raise
    return DemandaSustitucionesResult(
        anio_procesado=anio,
        version_algoritmo=ALGORITHM_VERSION,
        registros_al=registros_al,
        candidatas_unicas=unicas,
        candidatas_multiples=multiples,
        sin_candidata=sin_candidata,
        registros_agregados=agregados,
        relaciones_generadas=relaciones,
        duracion_segundos=perf_counter() - started,
        estado="success",
    )
