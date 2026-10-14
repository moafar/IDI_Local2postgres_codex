-- Propósito: definir las relaciones técnicas de sustitución de demanda.
-- Funcionamiento: crear una tabla anual de resultados, CHECK y un índice de consulta.
-- Dependencias: PostgreSQL y la variable psql schema, facilitada explícitamente.
-- Restricciones: ejecución manual autorizada; no crea esquemas ni modifica demanda.
-- Los identificadores no son clínicos. No ejecutar este DDL desde el pipeline.

CREATE TABLE :"schema".demanda_sustituciones (
    id_relacion text COLLATE "C" PRIMARY KEY,
    id_al text COLLATE "C" NOT NULL,
    id_candidata text COLLATE "C",
    any_prestacio smallint NOT NULL,
    pacient_nhc text COLLATE "C",
    nivell_1_codi text COLLATE "C",
    up_solicitant_corregida text COLLATE "C",
    data_prestacio text COLLATE "C",
    numero_prestacions_al bigint NOT NULL,
    prestacio_estat_codi_candidata text COLLATE "C",
    prestacio_nivell_9_codi_al text COLLATE "C",
    prestacio_nivell_9_codi_candidata text COLLATE "C",
    prestacio_nivell_9_descripcio_al text COLLATE "C",
    prestacio_nivell_9_descripcio_candidata text COLLATE "C",
    prestacio_descripcio_al text COLLATE "C",
    prestacio_descripcio_candidata text COLLATE "C",
    centre_codi_al text COLLATE "C",
    centre_codi_candidata text COLLATE "C",
    centre_sap_codi_al text COLLATE "C",
    centre_sap_codi_candidata text COLLATE "C",
    institucio_corregida_al text COLLATE "C",
    institucio_corregida_candidata text COLLATE "C",
    mismo_nivel_9 boolean,
    numero_candidatas bigint NOT NULL,
    clasificacion text COLLATE "C" NOT NULL,
    version_algoritmo text COLLATE "C" NOT NULL,
    fecha_carga timestamptz NOT NULL,

    CONSTRAINT ds_identificadores CHECK (
        id_relacion ~ '^[0-9a-f]{64}$'
        AND id_al ~ '^[0-9a-f]{64}$'
        AND (id_candidata IS NULL OR id_candidata ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ds_dominio CHECK (
        any_prestacio BETWEEN 1 AND 9999
        AND numero_prestacions_al > 0
        AND numero_candidatas >= 0
        AND version_algoritmo !~ '^[[:space:]]*$'
    ),
    CONSTRAINT ds_clasificacion CHECK (
        (
            id_candidata IS NOT NULL AND numero_prestacions_al = 1
            AND (
                (clasificacion = 'CANDIDATA_UNICA' AND numero_candidatas = 1)
                OR (clasificacion = 'CANDIDATAS_MULTIPLES' AND numero_candidatas > 1)
            )
        ) OR (
            id_candidata IS NULL AND numero_candidatas = 0
            AND (
                (clasificacion = 'SIN_CANDIDATA' AND numero_prestacions_al = 1)
                OR (clasificacion = 'REGISTRO_AGREGADO' AND numero_prestacions_al > 1)
            )
        )
    ),
    CONSTRAINT ds_candidata CHECK (
        (
            id_candidata IS NOT NULL
            AND prestacio_estat_codi_candidata IS NOT NULL
            AND prestacio_estat_codi_candidata IN ('CF', 'DI', 'EC', 'RE', 'SN', 'CO')
            AND pacient_nhc IS NOT NULL AND pacient_nhc !~ '^[[:space:]]*$'
            AND nivell_1_codi IS NOT NULL AND nivell_1_codi !~ '^[[:space:]]*$'
            AND up_solicitant_corregida IS NOT NULL
            AND up_solicitant_corregida !~ '^[[:space:]]*$'
            AND data_prestacio IS NOT NULL AND data_prestacio !~ '^[[:space:]]*$'
        ) OR (
            id_candidata IS NULL
            AND prestacio_estat_codi_candidata IS NULL
            AND prestacio_nivell_9_codi_candidata IS NULL
            AND prestacio_nivell_9_descripcio_candidata IS NULL
            AND prestacio_descripcio_candidata IS NULL
            AND centre_codi_candidata IS NULL
            AND centre_sap_codi_candidata IS NULL
            AND institucio_corregida_candidata IS NULL
        )
    ),
    CONSTRAINT ds_nivel_9 CHECK (
        mismo_nivel_9 IS NOT DISTINCT FROM CASE
            WHEN id_candidata IS NOT NULL
                AND prestacio_nivell_9_codi_al IS NOT NULL
                AND prestacio_nivell_9_codi_al !~ '^[[:space:]]*$'
                AND prestacio_nivell_9_codi_candidata IS NOT NULL
                AND prestacio_nivell_9_codi_candidata !~ '^[[:space:]]*$'
            THEN prestacio_nivell_9_codi_al = prestacio_nivell_9_codi_candidata
            ELSE NULL::boolean
        END
    )
);

CREATE INDEX demanda_sustituciones_anio_al_idx
    ON :"schema".demanda_sustituciones (any_prestacio, id_al);
