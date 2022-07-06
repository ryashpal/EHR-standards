import logging

log = logging.getLogger("Standardise")


def createMeasurementOperatorConcept(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_operator_concept")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_operator_concept cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_operator_concept AS
        SELECT
            vc.concept_name     AS source_code, -- operator_name,
            vc.concept_id       AS target_concept_id -- operator_concept_id
        FROM
            voc_dataset.concept vc
        WHERE
            vc.domain_id = 'Meas Value Operator'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementUnitTemp(con, schemaName):
    log.info("Creating table: " + schemaName + ".tmp_meas_unit")
    dropQuery = """drop table if exists """ + schemaName + """.tmp_meas_unit cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.tmp_meas_unit AS
        SELECT
            vc.concept_code                         AS concept_code,
            vc.vocabulary_id                        AS vocabulary_id,
            vc.domain_id                            AS domain_id,
            vc.concept_id                           AS concept_id,
            ROW_NUMBER() OVER (
                PARTITION BY vc.concept_code
                ORDER BY UPPER(vc.vocabulary_id)
            )                                       AS row_num -- for de-duplication
        FROM
            voc_dataset.concept vc
        WHERE
            vc.vocabulary_id IN ('UCUM', 'mimiciv_meas_unit', 'mimiciv_meas_wf_unit')
            AND vc.domain_id = 'Unit'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementUnitConcept(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_unit_concept")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_unit_concept cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_unit_concept AS
        SELECT
            vc.concept_code         AS source_code,
            vc.vocabulary_id        AS source_vocabulary_id,
            vc.domain_id            AS source_domain_id,
            vc.concept_id           AS source_concept_id,
            vc2.domain_id           AS target_domain_id,
            vc2.concept_id          AS target_concept_id
        FROM
            """ + schemaName + """.tmp_meas_unit vc
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.invalid_reason IS NULL
        WHERE
            vc.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def dropMeasurementUnitTemp(con, schemaName):
    log.info("Creating table: " + schemaName + ".tmp_meas_unit")
    dropQuery = """drop table if exists """ + schemaName + """.tmp_meas_unit cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createCharteventsClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_chartevents_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_chartevents_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_chartevents_clean AS
        SELECT
            src.subject_id                  AS subject_id,
            src.hadm_id                     AS hadm_id,
            src.stay_id                     AS stay_id,
            src.itemid                      AS itemid,
            CAST(src.itemid AS TEXT)      AS source_code,
            di.label                        AS source_label,
            src.charttime                   AS start_datetime,
            TRIM(src.value)                 AS value,
            CASE
                WHEN REGEXP_MATCH(TRIM(src.value), '^[-]?[\d]+[.]?[\d]*[ ]*[a-z]+$') IS NOT NULL THEN CAST((REGEXP_MATCH(src.value, '[-]?[\d]+[.]?[\d]*'))[1] AS FLOAT)
                ELSE src.valuenum																			   
            END                        AS valuenum,
            
            CASE
                WHEN REGEXP_MATCH(TRIM(src.value), '^[-]?[\d]+[.]?[\d]*[ ]*[a-z]+$') IS NOT NULL THEN REGEXP_MATCH(src.value, '[a-z]+')::character varying(20)
                ELSE src.valueuom
            END                AS valueuom, -- unit of measurement
            --
            'chartevents'           AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + schemaName + """.src_chartevents src -- ce
        INNER JOIN
            """ + schemaName + """.src_d_items di
                ON  src.itemid = di.itemid
        WHERE
            di.label NOT LIKE '%Temperature'
            OR di.label LIKE '%Temperature' 
            AND 
            CASE
                WHEN valueuom LIKE '%F%' THEN (valuenum - 32) * 5 / 9
                ELSE valuenum
            END BETWEEN 25 and 44
            
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCharteventsCodeTemp(con, schemaName):
    log.info("Creating table: " + schemaName + ".tmp_chartevents_code_dist")
    dropQuery = """drop table if exists """ + schemaName + """.tmp_chartevents_code_dist cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.tmp_chartevents_code_dist AS
        SELECT
            itemid                      AS itemid,
            source_code                 AS source_code,
            source_label                AS source_label,
            'mimiciv_meas_chart'        AS source_vocabulary_id,
            COUNT(*)                    AS row_count
        FROM
            """ + schemaName + """.lk_chartevents_clean
        GROUP BY
            itemid,
            source_code,
            source_label
        UNION ALL
        SELECT
            CAST(NULL AS INTEGER)                 AS itemid,
            value                               AS source_code,
            value                               AS source_label,
            'mimiciv_meas_chartevents_value'    AS source_vocabulary_id, -- both obs values and conditions
            COUNT(*)                            AS row_count
        FROM
            """ + schemaName + """.lk_chartevents_clean
        GROUP BY
            value,
            source_code,
            source_label
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCharteventsConcept(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_chartevents_concept")
    dropQuery = """drop table if exists """ + schemaName + """.lk_chartevents_concept cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_chartevents_concept AS
        SELECT
            src.itemid                  AS itemid,
            src.source_code             AS source_code,
            src.source_label            AS source_label,
            src.source_vocabulary_id    AS source_vocabulary_id,
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id,
            src.row_count               AS row_count
        FROM
            """ + schemaName + """.tmp_chartevents_code_dist src
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = src.source_code
                AND vc.vocabulary_id = src.source_vocabulary_id
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def dropCharteventsCodeTemp(con, schemaName):
    log.info("Creating table: " + schemaName + ".tmp_chartevents_code_dist")
    dropQuery = """drop table if exists """ + schemaName + """.tmp_chartevents_code_dist cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createCharteventsMapped(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_chartevents_mapped")
    dropQuery = """drop table if exists """ + schemaName + """.lk_chartevents_mapped cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_chartevents_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            src.stay_id                                 AS stay_id,
            src.start_datetime                          AS start_datetime,
            32817                                       AS type_concept_id,  -- OMOP4976890 EHR
            src.itemid                                  AS itemid,
            src.source_code                             AS source_code,
            src.source_label                            AS source_label,
            c_main.source_vocabulary_id                 AS source_vocabulary_id,
            c_main.source_domain_id                     AS source_domain_id,
            c_main.source_concept_id                    AS source_concept_id,
            c_main.target_domain_id                     AS target_domain_id,
            c_main.target_concept_id                    AS target_concept_id,
            CASE
                WHEN src.valuenum IS NULL THEN src.value 
                ELSE NULL
            END											 AS value_source_value,
            
            CASE
                WHEN  (src.valuenum IS NULL) AND (src.value IS NOT NULL)  THEN COALESCE(c_value.target_concept_id, 0)
                ELSE  NULL
            END 										 AS value_as_concept_id,

                src.valuenum                                AS value_as_number,
            src.valueuom                                AS unit_source_value, -- unit of measurement
        
        CASE
                WHEN src.valueuom IS NOT NULL THEN  COALESCE(uc.target_concept_id, 0)
                ELSE NULL								
            END 						AS unit_concept_id,
            CONCAT('meas.', src.unit_id)                AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + schemaName + """.lk_chartevents_clean src -- ce
        LEFT JOIN
            """ + schemaName + """.lk_chartevents_concept c_main -- main
                ON c_main.source_code = src.source_code 
                AND c_main.source_vocabulary_id = 'mimiciv_meas_chart'
        LEFT JOIN
            """ + schemaName + """.lk_chartevents_concept c_value -- values for main
                ON c_value.source_code = src.value
                AND c_value.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
                AND c_value.target_domain_id = 'Meas Value'
        LEFT JOIN 
            """ + schemaName + """.lk_meas_unit_concept uc
                ON uc.source_code = src.valueuom
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCharteventsConditionMapped(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_chartevents_condition_mapped")
    dropQuery = """drop table if exists """ + schemaName + """.lk_chartevents_condition_mapped cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_chartevents_condition_mapped AS
        SELECT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            src.stay_id                                 AS stay_id,
            src.start_datetime                          AS start_datetime,
            src.value                                   AS source_code,
            c_main.source_vocabulary_id                 AS source_vocabulary_id,
            c_main.source_concept_id                    AS source_concept_id,
            c_main.target_domain_id                     AS target_domain_id,
            c_main.target_concept_id                    AS target_concept_id,
            CONCAT('cond.', src.unit_id)                AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + schemaName + """.lk_chartevents_clean src -- ce
        INNER JOIN
            """ + schemaName + """.lk_chartevents_concept c_main -- condition domain from values, mapped
                ON c_main.source_code = src.value
                AND c_main.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
                AND c_main.target_domain_id = 'Condition'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLookupLabeventsClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_d_labitems_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_d_labitems_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_d_labitems_clean AS
        SELECT
            dlab.itemid                                                 AS itemid, -- for <cdm>.<source_value>
            COALESCE(dlab.loinc_code, 
                CAST(dlab.itemid AS TEXT))                            AS source_code, -- to join to vocabs
            dlab.loinc_code                                             AS loinc_code, -- for the crosswalk table
            CONCAT(dlab.label, '|', dlab.fluid, '|', dlab.category)     AS source_label, -- for the crosswalk table
            CASE 
            WHEN dlab.loinc_code IS NOT NULL THEN  'LOINC'
            ELSE 'mimiciv_meas_lab_loinc'
            END    
                                                        AS source_vocabulary_id
        FROM
            """ + schemaName + """.src_d_labitems dlab
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLabeventsClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_labevents_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_labevents_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_labevents_clean AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS measurement_id,
            src.subject_id                          AS subject_id,
            src.charttime                           AS start_datetime, -- measurement_datetime,
            src.hadm_id                             AS hadm_id,
            src.itemid                              AS itemid,
            src.value                               AS value, -- value_source_value
            REGEXP_MATCHES(src.value, '^(\<=|\>=|\>|\<|=|)')   AS value_operator,
            REGEXP_MATCHES(src.value, '[-]?[\d]+[.]?[\d]*')    AS value_number, -- assume "-0.34 etc"
            CASE
            WHEN TRIM(src.valueuom) <> '' THEN src  .valueuom 
            ELSE NULL    
            END AS valueuom, -- unit_source_value,
            src.ref_range_lower                     AS ref_range_lower,
            src.ref_range_upper                     AS ref_range_upper,
            'labevents'                             AS unit_id,
            --
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + schemaName + """.src_labevents src
        INNER JOIN
            """ + schemaName + """.src_d_labitems dlab
                ON src.itemid = dlab.itemid
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLookupLabitemsConcept(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_d_labitems_concept")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_d_labitems_concept cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_d_labitems_concept AS
        SELECT
            dlab.itemid                 AS itemid,
            dlab.source_code            AS source_code,
            dlab.loinc_code             AS loinc_code,
            dlab.source_label           AS source_label,
            dlab.source_vocabulary_id   AS source_vocabulary_id,
            -- source concept
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc.concept_name             AS source_concept_name,
            -- target concept
            vc2.vocabulary_id           AS target_vocabulary_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id,
            vc2.concept_name            AS target_concept_name,
            vc2.standard_concept        AS target_standard_concept
        FROM
            """ + schemaName + """.lk_meas_d_labitems_clean dlab
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = dlab.source_code -- join 
                AND vc.vocabulary_id = dlab.source_vocabulary_id
                -- AND vc.domain_id = 'Measurement'
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLabeventsWithId(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_labevents_hadm_id")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_labevents_hadm_id cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_labevents_hadm_id AS
        SELECT
            src.trace_id                        AS event_trace_id, 
            adm.hadm_id                         AS hadm_id,
            ROW_NUMBER() OVER (
                PARTITION BY src.trace_id
                ORDER BY adm.start_datetime
            )                                   AS row_num
        FROM  
            """ + schemaName + """.lk_meas_labevents_clean src
        INNER JOIN 
            """ + schemaName + """.lk_admissions_clean adm
                ON adm.subject_id = src.subject_id
                AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
        WHERE
            src.hadm_id IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLabeventsMapped(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_labevents_mapped")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_labevents_mapped cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_labevents_mapped AS
        SELECT
            src.measurement_id                      AS measurement_id,
            src.subject_id                          AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)     AS hadm_id,
            CAST(src.start_datetime AS DATE)        AS date_id,
            src.start_datetime                      AS start_datetime,
            src.itemid                              AS itemid,
            CAST(src.itemid AS TEXT)              AS source_code, -- change working source code to the representation
            labc.source_vocabulary_id               AS source_vocabulary_id,
            labc.source_concept_id                  AS source_concept_id,
            COALESCE(labc.target_domain_id, 'Measurement')  AS target_domain_id,
            labc.target_concept_id                  AS target_concept_id,
            src.valueuom                            AS unit_source_value,
            CASE 
            WHEN src.valueuom IS NOT NULL THEN COALESCE(uc.target_concept_id, 0)
            ELSE NULL
            END    AS unit_concept_id,
            src.value_operator                      AS operator_source_value,
            opc.target_concept_id                   AS operator_concept_id,
            src.value                               AS value_source_value,
            src.value_number                        AS value_as_number,
            CAST(NULL AS INTEGER)                     AS value_as_concept_id,
            src.ref_range_lower                     AS range_low,
            src.ref_range_upper                     AS range_high,
            CONCAT('meas.', src.unit_id)    AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM  
            """ + schemaName + """.lk_meas_labevents_clean src
        INNER JOIN 
            """ + schemaName + """.lk_meas_d_labitems_concept labc
                ON labc.itemid = src.itemid
        LEFT JOIN 
            """ + schemaName + """.lk_meas_operator_concept opc
                ON opc.source_code = src.value_operator[1]
        LEFT JOIN 
            """ + schemaName + """.lk_meas_unit_concept uc
                ON uc.source_code = src.valueuom
        LEFT JOIN 
            """ + schemaName + """.lk_meas_labevents_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroCrossReference(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_micro_cross_ref")
    dropQuery = """drop table if exists """ + schemaName + """.lk_micro_cross_ref cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_micro_cross_ref AS
        SELECT
            trace_id                                    AS trace_id_ab, -- for antibiotics
            FIRST_VALUE(src.trace_id) OVER (
                PARTITION BY
                    src.subject_id,
                    src.hadm_id,
                    COALESCE(src.charttime, src.chartdate),
                    src.spec_itemid,
                    src.test_itemid,
                    src.org_itemid
                ORDER BY src.trace_id
            )                                           AS trace_id_org, -- for test-organism pairs
            FIRST_VALUE(src.trace_id) OVER (
                PARTITION BY
                    src.subject_id,
                    src.hadm_id,
                    COALESCE(src.charttime, src.chartdate),
                    src.spec_itemid
                ORDER BY src.trace_id
            )                                           AS trace_id_spec, -- for specimen
            subject_id                                  AS subject_id,    -- to pick additional hadm_id from admissions
            hadm_id                                     AS hadm_id,
            COALESCE(src.charttime, src.chartdate)      AS start_datetime -- just to do coalesce once
        FROM
            """ + schemaName + """.src_microbiologyevents src -- mbe
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroWithId(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_micro_hadm_id")
    dropQuery = """drop table if exists """ + schemaName + """.lk_micro_hadm_id cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_micro_hadm_id AS
        SELECT
            src.trace_id_ab                     AS event_trace_id,
            adm.hadm_id                         AS hadm_id,
            ROW_NUMBER() OVER (
                PARTITION BY src.trace_id_ab
                ORDER BY adm.start_datetime
            )                                   AS row_num
        FROM  
            """ + schemaName + """.lk_micro_cross_ref src
        INNER JOIN 
            """ + schemaName + """.lk_admissions_clean adm
                ON adm.subject_id = src.subject_id
                AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
        WHERE
            src.hadm_id IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsOrganismClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_organism_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_organism_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_organism_clean AS
        SELECT DISTINCT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            cr.start_datetime                           AS start_datetime,
            src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
            src.test_itemid                             AS test_itemid, -- d_micro.itemid, test taken from the specimen
            src.org_itemid                              AS org_itemid, -- d_micro.itemid, organism which has grown
            cr.trace_id_spec                            AS trace_id_spec, -- to link org and spec in fact_relationship
            'micro.organism'                AS unit_id,
            src.load_table_id               AS load_table_id,
            0                               AS load_row_id,
            cr.trace_id_org                 AS trace_id         -- trace_id for test-organism
        FROM
            """ + schemaName + """.src_microbiologyevents src -- mbe
        INNER JOIN
            """ + schemaName + """.lk_micro_cross_ref cr
                ON src.trace_id = cr.trace_id_org
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createSpecimenClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_specimen_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_specimen_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_specimen_clean AS
        SELECT DISTINCT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            src.start_datetime                          AS start_datetime,
            src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
            -- 
            'micro.specimen'                AS unit_id,
            src.load_table_id               AS load_table_id,
            0                               AS load_row_id,
            cr.trace_id_spec                AS trace_id         -- trace_id for specimen
        FROM
            """ + schemaName + """.lk_meas_organism_clean src -- mbe
        INNER JOIN
            """ + schemaName + """.lk_micro_cross_ref cr
                ON src.trace_id = cr.trace_id_spec
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsAntibioticClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_ab_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_ab_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_ab_clean AS
        SELECT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            cr.start_datetime                           AS start_datetime,
            src.ab_itemid                               AS ab_itemid, -- antibiotic tested
            src.dilution_comparison                     AS dilution_comparison, -- operator sign
            src.dilution_value                          AS dilution_value, -- numeric dilution value
            src.interpretation                          AS interpretation, -- degree of resistance
            cr.trace_id_org                             AS trace_id_org, -- to link org to ab in fact_relationship
            -- 
            'micro.antibiotics'             AS unit_id,
            src.load_table_id               AS load_table_id,
            0                               AS load_row_id,
            src.trace_id                    AS trace_id         -- trace_id for antibiotics, no groupping is needed
        FROM
            """ + schemaName + """.src_microbiologyevents src
        INNER JOIN
            """ + schemaName + """.lk_micro_cross_ref cr
                ON src.trace_id = cr.trace_id_ab
        WHERE
            src.ab_itemid IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroLookupClean(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_d_micro_clean")
    dropQuery = """drop table if exists """ + schemaName + """.lk_d_micro_clean cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_d_micro_clean AS
        SELECT
            dm.itemid                                       AS itemid,
            CAST(dm.itemid AS TEXT)                       AS source_code,
            dm.label                                        AS source_label, -- for organism_mapped: test name plus specimen name
            CONCAT('mimiciv_micro_', LOWER(dm.category))    AS source_vocabulary_id
        FROM
            """ + schemaName + """.src_d_micro dm
        UNION ALL
        SELECT DISTINCT
            CAST(NULL AS INTEGER)                             AS itemid,
            src.interpretation                              AS source_code,
            src.interpretation                              AS source_label,
            'mimiciv_micro_resistance'                      AS source_vocabulary_id
        FROM
            """ + schemaName + """.lk_meas_ab_clean src
        WHERE
            src.interpretation IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroLookupConcept(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_d_micro_concept")
    dropQuery = """drop table if exists """ + schemaName + """.lk_d_micro_concept cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_d_micro_concept AS
        SELECT
            dm.itemid                   AS itemid,
            dm.source_code              AS source_code, -- itemid
            dm.source_label             AS source_label, -- symbolic information in case more mapping is required
            dm.source_vocabulary_id     AS source_vocabulary_id,
            -- source_concept
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc.concept_name             AS source_concept_name,
            -- target concept
            vc2.vocabulary_id           AS target_vocabulary_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id,
            vc2.concept_name            AS target_concept_name,
            vc2.standard_concept        AS target_standard_concept
        FROM
            """ + schemaName + """.lk_d_micro_clean dm
        LEFT JOIN
            voc_dataset.concept vc
                ON  dm.source_code = vc.concept_code
                -- gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen
                -- (gcpt) brand new vocab -> mimiciv_micro_test
                -- gcpt_org_name_to_concept -> mimiciv_micro_organism
                -- (gcpt) brand new vocab -> mimiciv_micro_resistance
                AND vc.vocabulary_id = dm.source_vocabulary_id
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createSpecimenMapped(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_specimen_mapped")
    dropQuery = """drop table if exists """ + schemaName + """.lk_specimen_mapped cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_specimen_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS specimen_id,
            src.subject_id                              AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
            CAST(src.start_datetime AS DATE)            AS date_id,
            32856                                       AS type_concept_id, -- Lab
            src.start_datetime                          AS start_datetime,
            src.spec_itemid                             AS spec_itemid,
            mc.source_code                              AS source_code,
            mc.source_vocabulary_id                     AS source_vocabulary_id,
            mc.source_concept_id                        AS source_concept_id,
            COALESCE(mc.target_domain_id, 'Specimen')   AS target_domain_id,
            mc.target_concept_id                        AS target_concept_id,
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + schemaName + """.lk_specimen_clean src
        INNER JOIN
            """ + schemaName + """.lk_d_micro_concept mc
                ON src.spec_itemid = mc.itemid
        LEFT JOIN
            """ + schemaName + """.lk_micro_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementOrganismMapped(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_organism_mapped")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_organism_mapped cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_organism_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
            src.subject_id                              AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
            CAST(src.start_datetime AS DATE)            AS date_id,
            32856                                       AS type_concept_id, -- Lab
            src.start_datetime                          AS start_datetime,
            src.test_itemid                             AS test_itemid,
            src.spec_itemid                             AS spec_itemid,
            src.org_itemid                              AS org_itemid,
            CONCAT(tc.source_code, '|', sc.source_code)     AS source_code, -- test itemid plus specimen itemid
            tc.source_vocabulary_id                     AS source_vocabulary_id,
            tc.source_concept_id                        AS source_concept_id,
            COALESCE(tc.target_domain_id, 'Measurement')    AS target_domain_id,
            tc.target_concept_id                        AS target_concept_id,
            oc.source_code                              AS value_source_value,
            oc.target_concept_id                        AS value_as_concept_id,
            src.trace_id_spec                           AS trace_id_spec,
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + schemaName + """.lk_meas_organism_clean src
        INNER JOIN
            """ + schemaName + """.lk_d_micro_concept tc
                ON src.test_itemid = tc.itemid
        INNER JOIN
            """ + schemaName + """.lk_d_micro_concept sc
                ON src.spec_itemid = sc.itemid
        LEFT JOIN
            """ + schemaName + """.lk_d_micro_concept oc
                ON src.org_itemid = oc.itemid
        LEFT JOIN
            """ + schemaName + """.lk_micro_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementAntibioticMapped(con, schemaName):
    log.info("Creating table: " + schemaName + ".lk_meas_ab_mapped")
    dropQuery = """drop table if exists """ + schemaName + """.lk_meas_ab_mapped cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.lk_meas_ab_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
            src.subject_id                              AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
            CAST(src.start_datetime AS DATE)            AS date_id,
            32856                                       AS type_concept_id, -- Lab
            src.start_datetime                          AS start_datetime,
            src.ab_itemid                               AS ab_itemid,
            ac.source_code                              AS source_code,
            COALESCE(ac.target_concept_id, 0)           AS target_concept_id,
            COALESCE(ac.source_concept_id, 0)           AS source_concept_id,
            rc.target_concept_id                        AS value_as_concept_id,
            src.interpretation                          AS value_source_value,
            src.dilution_value                          AS value_as_number,
            src.dilution_comparison                     AS operator_source_value,
            opc.target_concept_id                       AS operator_concept_id,
            COALESCE(ac.target_domain_id, 'Measurement')    AS target_domain_id,
            -- fields to link test-organism and antibiotics
            src.trace_id_org                            AS trace_id_org,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + schemaName + """.lk_meas_ab_clean src
        INNER JOIN
            """ + schemaName + """.lk_d_micro_concept ac
                ON src.ab_itemid = ac.itemid
        LEFT JOIN
            """ + schemaName + """.lk_d_micro_concept rc
                ON src.interpretation = rc.source_code
                AND rc.source_vocabulary_id = 'mimiciv_micro_resistance' -- new vocab
        LEFT JOIN
            """ + schemaName + """.lk_meas_operator_concept opc -- see lk_meas_labevents.sql
                ON src.dilution_comparison = opc.source_code
        LEFT JOIN
            """ + schemaName + """.lk_micro_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def migrateUnits(con, schemaName):
    createMeasurementOperatorConcept(con = con, schemaName = schemaName)
    createMeasurementUnitTemp(con = con, schemaName = schemaName)
    createMeasurementUnitConcept(con = con, schemaName = schemaName)
    dropMeasurementUnitTemp(con = con, schemaName = schemaName)


def migrateChartevents(con, schemaName):
    createCharteventsClean(con = con, schemaName = schemaName)
    createCharteventsCodeTemp(con = con, schemaName = schemaName)
    createCharteventsConcept(con = con, schemaName = schemaName)
    dropCharteventsCodeTemp(con = con, schemaName = schemaName)
    createCharteventsMapped(con = con, schemaName = schemaName)
    createCharteventsConditionMapped(con = con, schemaName = schemaName)


def migrateLabevents(con, schemaName):
    createMeasurementsLookupLabeventsClean(con = con, schemaName = schemaName)
    createMeasurementsLabeventsClean(con = con, schemaName = schemaName)
    createMeasurementsLookupLabitemsConcept(con = con, schemaName = schemaName)
    createMeasurementsLabeventsWithId(con = con, schemaName = schemaName)
    createMeasurementsLabeventsMapped(con = con, schemaName = schemaName)


def migrateSpecimen(con, schemaName):
    createMicroCrossReference(con = con, schemaName = schemaName)
    createMicroWithId(con = con, schemaName = schemaName)
    createMeasurementsOrganismClean(con = con, schemaName = schemaName)
    createSpecimenClean(con = con, schemaName = schemaName)
    createMeasurementsAntibioticClean(con = con, schemaName = schemaName)
    createMicroLookupClean(con = con, schemaName = schemaName)
    createMicroLookupConcept(con = con, schemaName = schemaName)
    createSpecimenMapped(con = con, schemaName = schemaName)
    createMeasurementOrganismMapped(con = con, schemaName = schemaName)
    createMeasurementAntibioticMapped(con = con, schemaName = schemaName)
