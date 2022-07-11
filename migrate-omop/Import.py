import logging

log = logging.getLogger("Standardise")

import Config


def __saveDataframe(con, destinationSchemaName, destinationTableName, df, dfColumns):

    import numpy as np
    import psycopg2.extras
    import psycopg2.extensions

    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

    log.info("Importing data to table: " + destinationSchemaName + '.' + destinationTableName)

    if len(df) > 0:
        table = destinationSchemaName + '.' + destinationTableName
        columns = '"' + '", "'.join(dfColumns) + '"'
        values = "VALUES({})".format(",".join(["%s" for _ in dfColumns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        try:
            cur = con.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df[dfColumns].values)
            con.commit()
        finally:
            cur.close()


def importPatients(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".PATIENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.PATIENTS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.PATIENTS
        (
            SUBJECT_ID INT NOT NULL,
            GENDER VARCHAR(5) NOT NULL,
            ANCHOR_AGE INT NOT NULL,
            ANCHOR_YEAR INT NOT NULL,
            ANCHOR_YEAR_GROUP VARCHAR(12) NOT NULL,
            DOD TIMESTAMP(0), -- This is a NaN column

            CONSTRAINT pat_subid_unique UNIQUE (SUBJECT_ID),
            CONSTRAINT pat_subid_pk PRIMARY KEY (SUBJECT_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.dod.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['gender'],
        Config.patients['column_mapping']['anchor_age'],
        Config.patients['column_mapping']['anchor_year'],
        Config.patients['column_mapping']['anchor_year_group'],
        Config.patients['column_mapping']['dod'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='PATIENTS', df=df, dfColumns=dfColumns)


def importAdmissions(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".ADMISSIONS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.ADMISSIONS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.ADMISSIONS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            ADMITTIME TIMESTAMP(0) NOT NULL,
            DISCHTIME TIMESTAMP(0) NOT NULL,
            DEATHTIME TIMESTAMP(0),
            ADMISSION_TYPE VARCHAR(50) NOT NULL,
            ADMISSION_LOCATION VARCHAR(50), -- There is NULL in this version
            DISCHARGE_LOCATION VARCHAR(50), -- There is NULL in this version
            INSURANCE VARCHAR(255) NOT NULL,
            LANGUAGE VARCHAR(10),
            MARITAL_STATUS VARCHAR(50),
            ETHNICITY VARCHAR(200) NOT NULL,
            EDREGTIME TIMESTAMP(0),
            EDOUTTIME TIMESTAMP(0),
            HOSPITAL_EXPIRE_FLAG SMALLINT,
            CONSTRAINT adm_hadm_pk PRIMARY KEY (HADM_ID),
            CONSTRAINT adm_hadm_unique UNIQUE (HADM_ID)

        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.deathtime.replace({np.nan: None}, inplace=True)
    df.edregtime.replace({np.nan: None}, inplace=True)
    df.edouttime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['admittime'],
        Config.patients['column_mapping']['dischtime'],
        Config.patients['column_mapping']['deathtime'],
        Config.patients['column_mapping']['admission_type'],
        Config.patients['column_mapping']['admission_location'],
        Config.patients['column_mapping']['discharge_location'],
        Config.patients['column_mapping']['insurance'],
        Config.patients['column_mapping']['language'],
        Config.patients['column_mapping']['marital_status'],
        Config.patients['column_mapping']['ethnicity'],
        Config.patients['column_mapping']['edregtime'],
        Config.patients['column_mapping']['edouttime'],
        Config.patients['column_mapping']['hospital_expire_flag'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='ADMISSIONS', df=df, dfColumns=dfColumns)


def importTransfers(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".TRANSFERS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.TRANSFERS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.TRANSFERS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT,
            TRANSFER_ID INT NOT NULL,
            EVENTTYPE VARCHAR(20) NOT NULL,
            CAREUNIT VARCHAR(50),
            INTIME TIMESTAMP(0),
            OUTTIME TIMESTAMP(0),
            CONSTRAINT transfers_subid_transid_pk PRIMARY KEY (SUBJECT_ID, TRANSFER_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.intime.replace({np.nan: None}, inplace=True)
    df.outtime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['transfer_id'],
        Config.patients['column_mapping']['eventtype'],
        Config.patients['column_mapping']['careunit'],
        Config.patients['column_mapping']['intime'],
        Config.patients['column_mapping']['outtime'],
        ]
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='TRANSFERS', df=df, dfColumns=dfColumns)


def importDiagnosesIcd(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".DIAGNOSES_ICD")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.DIAGNOSES_ICD CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.DIAGNOSES_ICD
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            SEQ_NUM INT NOT NULL,
            ICD_CODE VARCHAR(10) NOT NULL,
            ICD_VERSION INT NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['seq_num'],
        Config.patients['column_mapping']['icd_code'],
        Config.patients['column_mapping']['icd_version'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='DIAGNOSES_ICD', df=df, dfColumns=dfColumns)


def importServices(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".SERVICES")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.SERVICES CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.SERVICES
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            TRANSFERTIME TIMESTAMP(0) NOT NULL,
            PREV_SERVICE VARCHAR(20) ,
            CURR_SERVICE VARCHAR(20) NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.transfertime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['transfertime'],
        Config.patients['column_mapping']['prev_service'],
        Config.patients['column_mapping']['curr_service'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='SERVICES', df=df, dfColumns=dfColumns)


def importLabEvents(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".LABEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.LABEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.LABEVENTS
        (
            LABEVENT_ID INT NOT NULL,
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT ,
            SPECIMEN_ID INT NOT NULL,
            ITEMID INT NOT NULL,
            CHARTTIME TIMESTAMP NOT NULL,
            STORETIME TIMESTAMP ,
            VALUE VARCHAR(200) ,
            VALUENUM DOUBLE PRECISION ,
            VALUEUOM VARCHAR(20) ,
            REF_RANGE_LOWER DOUBLE PRECISION ,
            REF_RANGE_UPPER  DOUBLE PRECISION,
            FLAG VARCHAR(10) ,
            PRIORITY VARCHAR(7) ,
            COMMENTS VARCHAR(620) ,
            CONSTRAINT labevents_labeventid_pk PRIMARY KEY (LABEVENT_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['labevent_id'],
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['specimen_id'],
        Config.patients['column_mapping']['itemid'],
        Config.patients['column_mapping']['charttime'],
        Config.patients['column_mapping']['storetime'],
        Config.patients['column_mapping']['value'],
        Config.patients['column_mapping']['valuenum'],
        Config.patients['column_mapping']['valueuom'],
        Config.patients['column_mapping']['ref_range_lower'],
        Config.patients['column_mapping']['ref_range_upper'],
        Config.patients['column_mapping']['flag'],
        Config.patients['column_mapping']['priority'],
        Config.patients['column_mapping']['comments'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='LABEVENTS', df=df, dfColumns=dfColumns)


def importLabItems(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".D_LABITEMS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.D_LABITEMS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.D_LABITEMS
        (
            ITEMID INT NOT NULL,
            LABEL VARCHAR(50),
            FLUID VARCHAR(50) NOT NULL,
            CATEGORY VARCHAR(50) NOT NULL,
            LOINC_CODE VARCHAR(50),
            CONSTRAINT d_labitems_itemid_pk PRIMARY KEY (ITEMID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['itemid'],
        Config.patients['column_mapping']['label'],
        Config.patients['column_mapping']['fluid'],
        Config.patients['column_mapping']['category'],
        Config.patients['column_mapping']['loinc_code'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='D_LABITEMS', df=df, dfColumns=dfColumns)


def importProcedures(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".PROCEDURES_ICD")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.PROCEDURES_ICD CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.PROCEDURES_ICD
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            SEQ_NUM INT NOT NULL,
            CHARTDATE TIMESTAMP(0) NOT NULL,
            ICD_CODE VARCHAR(10) NOT NULL,
            ICD_VERSION INT NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['seq_num'],
        Config.patients['column_mapping']['chartdate'],
        Config.patients['column_mapping']['icd_code'],
        Config.patients['column_mapping']['icd_version'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='PROCEDURES_ICD', df=df, dfColumns=dfColumns)


def importHcpcsEvents(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".HCPCSEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.HCPCSEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.HCPCSEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            CHARTDATE TIMESTAMP(0) NOT NULL, -- new for 1.0
            HCPCS_CD VARCHAR(5) NOT NULL,
            SEQ_NUM INT NOT NULL,
            SHORT_DESCRIPTION VARCHAR(170) NOT NULL
            -- longest is 165
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['chartdate'],
        Config.patients['column_mapping']['hcpcs_cd'],
        Config.patients['column_mapping']['seq_num'],
        Config.patients['column_mapping']['short_description'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='HCPCSEVENTS', df=df, dfColumns=dfColumns)


def importDrugCodes(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".DRGCODES")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.DRGCODES CASCADE"""
    createQuery = """CREATE TABLE  """ + destinationSchemaName + """.DRGCODES
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            DRG_TYPE VARCHAR(4) NOT NULL,
            DRG_CODE VARCHAR(10) NOT NULL,
            DESCRIPTION VARCHAR(195) ,
            DRG_SEVERITY SMALLINT,
            DRG_MORTALITY SMALLINT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['drg_severity'] = df['drg_severity'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['drg_mortality'] = df['drg_mortality'].astype('Int64').fillna(0).astype('int').replace({0: None})
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['drg_type'],
        Config.patients['column_mapping']['drg_code'],
        Config.patients['column_mapping']['description'],
        Config.patients['column_mapping']['drg_severity'],
        Config.patients['column_mapping']['drg_mortality'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='DRGCODES', df=df, dfColumns=dfColumns)


def importPrescriptions(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".PRESCRIPTIONS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.PRESCRIPTIONS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.PRESCRIPTIONS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            PHARMACY_ID INT NOT NULL,
            STARTTIME TIMESTAMP(0) ,
            STOPTIME TIMESTAMP(0) ,
            DRUG_TYPE VARCHAR(10) NOT NULL,
            DRUG VARCHAR(100) ,
            GSN VARCHAR(250) , -- exceeds 10
            NDC VARCHAR(20) ,
            PROD_STRENGTH VARCHAR(120) ,
            FORM_RX  VARCHAR(10),
            DOSE_VAL_RX VARCHAR(50) ,
            DOSE_UNIT_RX VARCHAR(50) ,
            FORM_VAL_DISP VARCHAR(30) ,
            FORM_UNIT_DISP VARCHAR(30) ,
            DOSES_PER_24_HRS DOUBLE PRECISION ,
            ROUTE VARCHAR(30)

        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.stoptime.replace({np.nan: None}, inplace=True)
    df.starttime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['pharmacy_id'],
        Config.patients['column_mapping']['starttime'],
        Config.patients['column_mapping']['stoptime'],
        Config.patients['column_mapping']['drug_type'],
        Config.patients['column_mapping']['drug'],
        Config.patients['column_mapping']['gsn'],
        Config.patients['column_mapping']['ndc'],
        Config.patients['column_mapping']['prod_strength'],
        Config.patients['column_mapping']['form_rx'],
        Config.patients['column_mapping']['dose_val_rx'],
        Config.patients['column_mapping']['dose_unit_rx'],
        Config.patients['column_mapping']['form_val_disp'],
        Config.patients['column_mapping']['form_unit_disp'],
        Config.patients['column_mapping']['doses_per_24_hrs'],
        Config.patients['column_mapping']['route'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='PRESCRIPTIONS', df=df, dfColumns=dfColumns)


def importMicrobiologyEvents(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".MICROBIOLOGYEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.MICROBIOLOGYEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.MICROBIOLOGYEVENTS
        (
            MICROEVENT_ID INT NOT NULL,
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT ,
            MICRO_SPECIMEN_ID INT NOT NULL,
            CHARTDATE TIMESTAMP(0) NOT NULL,
            CHARTTIME TIMESTAMP(0) ,
            SPEC_ITEMID INT NOT NULL,
            SPEC_TYPE_DESC VARCHAR(100) NOT NULL,
            TEST_SEQ INT NOT NULL,
            STOREDATE TIMESTAMP(0) ,
            STORETIME TIMESTAMP(0) ,
            TEST_ITEMID INT NOT NULL,
            TEST_NAME VARCHAR(100) NOT NULL,
            ORG_ITEMID INT ,
            ORG_NAME VARCHAR(100) ,
            ISOLATE_NUM SMALLINT ,
            QUANTITY VARCHAR(50) ,
            AB_ITEMID INT ,
            AB_NAME VARCHAR(30) ,
            DILUTION_TEXT VARCHAR(10) ,
            DILUTION_COMPARISON VARCHAR(20) ,
            DILUTION_VALUE DOUBLE PRECISION ,
            INTERPRETATION VARCHAR(5) ,
            COMMENTS VARCHAR(750),
            CONSTRAINT mbe_microevent_id_pk PRIMARY KEY (MICROEVENT_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['org_itemid'] = df['org_itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['ab_itemid'] = df['ab_itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['isolate_num'] = df['isolate_num'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.chartdate.replace({np.nan: None}, inplace=True)
    df.charttime.replace({np.nan: None}, inplace=True)
    df.storedate.replace({np.nan: None}, inplace=True)
    df.storetime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['microevent_id'],
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['micro_specimen_id'],
        Config.patients['column_mapping']['chartdate'],
        Config.patients['column_mapping']['charttime'],
        Config.patients['column_mapping']['spec_itemid'],
        Config.patients['column_mapping']['spec_type_desc'],
        Config.patients['column_mapping']['test_seq'],
        Config.patients['column_mapping']['storedate'],
        Config.patients['column_mapping']['storetime'],
        Config.patients['column_mapping']['test_itemid'],
        Config.patients['column_mapping']['test_name'],
        Config.patients['column_mapping']['org_itemid'],
        Config.patients['column_mapping']['org_name'],
        Config.patients['column_mapping']['isolate_num'],
        Config.patients['column_mapping']['quantity'],
        Config.patients['column_mapping']['ab_itemid'],
        Config.patients['column_mapping']['ab_name'],
        Config.patients['column_mapping']['dilution_text'],
        Config.patients['column_mapping']['dilution_comparison'],
        Config.patients['column_mapping']['dilution_value'],
        Config.patients['column_mapping']['interpretation'],
        Config.patients['column_mapping']['comments'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='MICROBIOLOGYEVENTS', df=df, dfColumns=dfColumns)


def importMicrobiologyItems(con, destinationSchemaName, filePath, fileSeparator):
    pass


def importPharmacy(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".PHARMACY")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.PHARMACY CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.PHARMACY
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            PHARMACY_ID INT NOT NULL,
            POE_ID VARCHAR(25) ,
            STARTTIME TIMESTAMP(0) ,
            STOPTIME TIMESTAMP(0) ,
            MEDICATION VARCHAR(100) ,
            PROC_TYPE VARCHAR(50) NOT NULL,
            STATUS VARCHAR(50) NOT NULL,
            ENTERTIME TIMESTAMP(0) NOT NULL,
            VERIFIEDTIME TIMESTAMP(0) ,
            ROUTE VARCHAR(30),
            FREQUENCY VARCHAR(30) ,
            DISP_SCHED VARCHAR(100) ,
            INFUSION_TYPE VARCHAR(15) ,
            SLIDING_SCALE VARCHAR(5) ,
            LOCKOUT_INTERVAL VARCHAR(50) ,
            BASAL_RATE  DOUBLE PRECISION,
            ONE_HR_MAX  VARCHAR(30), 
            DOSES_PER_24_HRS DOUBLE PRECISION ,
            DURATION DOUBLE PRECISION,
            DURATION_INTERVAL VARCHAR(50) ,
            EXPIRATION_VALUE INT ,
            EXPIRATION_UNIT VARCHAR(50) ,
            EXPIRATIONDATE TIMESTAMP(0) ,
            DISPENSATION VARCHAR(50) ,
            FILL_QUANTITY VARCHAR(30),
            CONSTRAINT pharmacy_pharmacy_pk PRIMARY KEY (PHARMACY_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['subject_id'] = df['subject_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['pharmacy_id'] = df['pharmacy_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['expiration_value'] = df['expiration_value'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.starttime.replace({np.nan: None}, inplace=True)
    df.stoptime.replace({np.nan: None}, inplace=True)
    df.entertime.replace({np.nan: None}, inplace=True)
    df.verifiedtime.replace({np.nan: None}, inplace=True)
    df.expirationdate.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['pharmacy_id'],
        Config.patients['column_mapping']['poe_id'],
        Config.patients['column_mapping']['starttime'],
        Config.patients['column_mapping']['stoptime'],
        Config.patients['column_mapping']['medication'],
        Config.patients['column_mapping']['proc_type'],
        Config.patients['column_mapping']['status'],
        Config.patients['column_mapping']['entertime'],
        Config.patients['column_mapping']['verifiedtime'],
        Config.patients['column_mapping']['route'],
        Config.patients['column_mapping']['frequency'],
        Config.patients['column_mapping']['disp_sched'],
        Config.patients['column_mapping']['infusion_type'],
        Config.patients['column_mapping']['sliding_scale'],
        Config.patients['column_mapping']['lockout_interval'],
        Config.patients['column_mapping']['basal_rate'],
        Config.patients['column_mapping']['one_hr_max'],
        Config.patients['column_mapping']['doses_per_24_hrs'],
        Config.patients['column_mapping']['duration'],
        Config.patients['column_mapping']['duration_interval'],
        Config.patients['column_mapping']['expiration_value'],
        Config.patients['column_mapping']['expiration_unit'],
        Config.patients['column_mapping']['expirationdate'],
        Config.patients['column_mapping']['dispensation'],
        Config.patients['column_mapping']['fill_quantity'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='PHARMACY', df=df, dfColumns=dfColumns)


def importProcedureEvents(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".PROCEDUREEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.PROCEDUREEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.PROCEDUREEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            STAY_ID INT NOT NULL,
            STARTTIME TIMESTAMP(0) NOT NULL,
            ENDTIME TIMESTAMP(0) NOT NULL,
            STORETIME TIMESTAMP(0) NOT NULL,
            ITEMID INT NOT NULL,
            VALUE DOUBLE PRECISION NOT NULL,
            VALUEUOM VARCHAR(30) NOT NULL,
            LOCATION VARCHAR(30),
            LOCATIONCATEGORY VARCHAR(30),
            ORDERID INT NOT NULL,
            LINKORDERID INT NOT NULL,
            ORDERCATEGORYNAME VARCHAR(100) NOT NULL,
            SECONDARYORDERCATEGORYNAME VARCHAR(100),
            ORDERCATEGORYDESCRIPTION VARCHAR(50) NOT NULL,
            PATIENTWEIGHT DOUBLE PRECISION NOT NULL,
            TOTALAMOUNT DOUBLE PRECISION,
            TOTALAMOUNTUOM VARCHAR(50),
            ISOPENBAG SMALLINT NOT NULL,
            CONTINUEINNEXTDEPT SMALLINT NOT NULL,
            CANCELREASON SMALLINT NOT NULL,
            STATUSDESCRIPTION VARCHAR(30) NOT NULL,
            COMMENTS_DATE TIMESTAMP(0),
            ORIGINALAMOUNT DOUBLE PRECISION NOT NULL,
            ORIGINALRATE DOUBLE PRECISION NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['stay_id'] = df['stay_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['itemid'] = df['itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['orderid'] = df['orderid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['linkorderid'] = df['linkorderid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.starttime.replace({np.nan: None}, inplace=True)
    df.endtime.replace({np.nan: None}, inplace=True)
    df.storetime.replace({np.nan: None}, inplace=True)
    df.comments_date.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['stay_id'],
        Config.patients['column_mapping']['starttime'],
        Config.patients['column_mapping']['endtime'],
        Config.patients['column_mapping']['storetime'],
        Config.patients['column_mapping']['itemid'],
        Config.patients['column_mapping']['value'],
        Config.patients['column_mapping']['valueuom'],
        Config.patients['column_mapping']['location'],
        Config.patients['column_mapping']['locationcategory'],
        Config.patients['column_mapping']['orderid'],
        Config.patients['column_mapping']['linkorderid'],
        Config.patients['column_mapping']['ordercategoryname'],
        Config.patients['column_mapping']['secondaryordercategoryname'],
        Config.patients['column_mapping']['ordercategorydescription'],
        Config.patients['column_mapping']['patientweight'],
        Config.patients['column_mapping']['totalamount'],
        Config.patients['column_mapping']['totalamountuom'],
        Config.patients['column_mapping']['isopenbag'],
        Config.patients['column_mapping']['continueinnextdept'],
        Config.patients['column_mapping']['cancelreason'],
        Config.patients['column_mapping']['statusdescription'],
        Config.patients['column_mapping']['comments_date'],
        Config.patients['column_mapping']['originalamount'],
        Config.patients['column_mapping']['originalrate'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='PROCEDUREEVENTS', df=df, dfColumns=dfColumns)


def importItems(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".D_ITEMS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.D_ITEMS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.D_ITEMS
        (
            ITEMID INT NOT NULL,
            LABEL VARCHAR(200) NOT NULL,
            ABBREVIATION VARCHAR(100) NOT NULL,
            LINKSTO VARCHAR(50) NOT NULL,
            CATEGORY VARCHAR(100) NOT NULL,
            UNITNAME VARCHAR(100),
            PARAM_TYPE VARCHAR(30) NOT NULL,
            LOWNORMALVALUE DOUBLE PRECISION,
            HIGHNORMALVALUE DOUBLE PRECISION,
            CONSTRAINT ditems_itemid_unique UNIQUE (ITEMID),
            CONSTRAINT ditems_itemid_pk PRIMARY KEY (ITEMID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['itemid'],
        Config.patients['column_mapping']['label'],
        Config.patients['column_mapping']['abbreviation'],
        Config.patients['column_mapping']['linksto'],
        Config.patients['column_mapping']['category'],
        Config.patients['column_mapping']['unitname'],
        Config.patients['column_mapping']['param_type'],
        Config.patients['column_mapping']['lownormalvalue'],
        Config.patients['column_mapping']['highnormalvalue'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='D_ITEMS', df=df, dfColumns=dfColumns)


def importDatetimeEvents(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".DATETIMEEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.DATETIMEEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.DATETIMEEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT,
            STAY_ID INT,
            CHARTTIME TIMESTAMP(0) NOT NULL,
            STORETIME TIMESTAMP(0) NOT NULL,
            ITEMID INT NOT NULL,
            VALUE TIMESTAMP(0) NOT NULL,
            VALUEUOM VARCHAR(50) NOT NULL,
            WARNING SMALLINT NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['stay_id'],
        Config.patients['column_mapping']['charttime'],
        Config.patients['column_mapping']['storetime'],
        Config.patients['column_mapping']['itemid'],
        Config.patients['column_mapping']['value'],
        Config.patients['column_mapping']['valueuom'],
        Config.patients['column_mapping']['warning'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='DATETIMEEVENTS', df=df, dfColumns=dfColumns)


def importChartEvents(con, destinationSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + destinationSchemaName + ".CHARTEVENTS")

    # dropQuery = """DROP TABLE IF EXISTS """ + destinationSchemaName + """.CHARTEVENTS CASCADE"""
    # createQuery = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS
    #     (
    #         SUBJECT_ID INT NOT NULL,
    #         HADM_ID INT NOT NULL,
    #         STAY_ID INT NOT NULL,
    #         CHARTTIME TIMESTAMP(0) NOT NULL,
    #         STORETIME TIMESTAMP(0) ,
    #         ITEMID INT NOT NULL,
    #         VALUE VARCHAR(160) ,
    #         VALUENUM DOUBLE PRECISION,
    #         VALUEUOM VARCHAR(20),
    #         WARNING SMALLINT NOT NULL
    #     )
    #     ;
    #     """
    # createChildQuery1 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_1 ( CHECK ( itemid >= 220000 AND itemid < 221000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery2 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_2 ( CHECK ( itemid >= 221000 AND itemid < 222000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery3 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_3 ( CHECK ( itemid >= 222000 AND itemid < 223000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery4 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_4 ( CHECK ( itemid >= 223000 AND itemid < 224000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery5 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_5 ( CHECK ( itemid >= 224000 AND itemid < 225000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery6 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_6 ( CHECK ( itemid >= 225000 AND itemid < 226000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery7 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_7 ( CHECK ( itemid >= 226000 AND itemid < 227000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery8 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_8 ( CHECK ( itemid >= 227000 AND itemid < 228000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery9 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_9 ( CHECK ( itemid >= 228000 AND itemid < 229000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createChildQuery10 = """CREATE TABLE """ + destinationSchemaName + """.CHARTEVENTS_10 ( CHECK ( itemid >= 229000 AND itemid < 230000 )) INHERITS (""" + destinationSchemaName + """.CHARTEVENTS);"""
    # createFunctionQuery = """CREATE OR REPLACE FUNCTION """ + destinationSchemaName + """.chartevents_insert_trigger()
    #     RETURNS TRIGGER AS $$
    #     BEGIN
    #     IF ( NEW.itemid >= 220000 AND NEW.itemid < 221000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_1 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 221000 AND NEW.itemid < 222000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_2 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 222000 AND NEW.itemid < 223000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_3 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 223000 AND NEW.itemid < 224000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_4 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 224000 AND NEW.itemid < 225000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_5 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 225000 AND NEW.itemid < 226000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_6 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 226000 AND NEW.itemid < 227000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_7 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 227000 AND NEW.itemid < 228000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_8 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 228000 AND NEW.itemid < 229000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_9 VALUES (NEW.*);
    #     ELSIF ( NEW.itemid >= 229000 AND NEW.itemid < 230000 ) THEN INSERT INTO """ + destinationSchemaName + """.CHARTEVENTS_10 VALUES (NEW.*);
    #     ELSE
    #         INSERT INTO """ + destinationSchemaName + """.chartevents_null VALUES (NEW.*);
    #     END IF;
    #     RETURN NULL;
    #     END;
    #     $$
    #     LANGUAGE plpgsql
    #     ;
    #     """
    # dropTriggerQuery = """DROP TRIGGER IF EXISTS insert_chartevents_trigger
    # ON """ + destinationSchemaName + """.CHARTEVENTS
    # ;
    # """
    # createTriggerQuery = """CREATE TRIGGER insert_chartevents_trigger
    # BEFORE INSERT ON """ + destinationSchemaName + """.CHARTEVENTS
    # FOR EACH ROW EXECUTE PROCEDURE """ + destinationSchemaName + """.chartevents_insert_trigger()
    # ;
    # """
    # with con:
    #     with con.cursor() as cursor:
    #         cursor.execute(dropQuery)
    #         cursor.execute(createQuery)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_1")
    #         cursor.execute(createChildQuery1)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_2")
    #         cursor.execute(createChildQuery2)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_3")
    #         cursor.execute(createChildQuery3)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_4")
    #         cursor.execute(createChildQuery4)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_5")
    #         cursor.execute(createChildQuery5)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_6")
    #         cursor.execute(createChildQuery6)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_7")
    #         cursor.execute(createChildQuery7)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_8")
    #         cursor.execute(createChildQuery8)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_9")
    #         cursor.execute(createChildQuery9)
    #         log.info("Creating child table: " + destinationSchemaName + ".CHARTEVENTS_10")
    #         cursor.execute(createChildQuery10)
    #         log.info("Creating function: " + destinationSchemaName + ".CHARTEVENTS.chartevents_insert_trigger()")
    #         cursor.execute(createFunctionQuery)
    #         log.info("Dropping trigger: " + destinationSchemaName + ".CHARTEVENTS.insert_chartevents_trigger")
    #         cursor.execute(dropTriggerQuery)
    #         log.info("Creating trigger: " + destinationSchemaName + ".CHARTEVENTS.insert_chartevents_trigger")
    #         cursor.execute(createTriggerQuery)

    import pandas as pd

    log.info("Reading file: " + str(filePath))
    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['hadm_id'],
        Config.patients['column_mapping']['stay_id'],
        Config.patients['column_mapping']['charttime'],
        Config.patients['column_mapping']['storetime'],
        Config.patients['column_mapping']['itemid'],
        Config.patients['column_mapping']['value'],
        Config.patients['column_mapping']['valuenum'],
        Config.patients['column_mapping']['valueuom'],
        Config.patients['column_mapping']['warning'],
        ]
    __saveDataframe(con=con, destinationSchemaName=destinationSchemaName, destinationTableName='CHARTEVENTS', df=df, dfColumns=dfColumns)


def importCsv(con, destinationSchemaName):
    importPatients(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.patients['file_name'],
        fileSeparator=','
        )
    importAdmissions(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.admission['file_name'],
        fileSeparator=','
        )
    importTransfers(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.transfers['file_name'],
        fileSeparator=','
        )
    importDiagnosesIcd(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.diagnoses_icd['file_name'],
        fileSeparator=','
        )
    importServices(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.services['file_name'],
        fileSeparator=','
        )
    importLabEvents(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.labevents['file_name'],
        fileSeparator=','
        )
    importLabItems(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.d_labitems['file_name'],
        fileSeparator=','
        )
    importProcedures(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.procedures_icd['file_name'],
        fileSeparator=','
        )
    importHcpcsEvents(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.hcpcsevents['file_name'],
        fileSeparator=','
        )
    importDrugCodes(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.drgcodes['file_name'],
        fileSeparator=','
        )
    importPrescriptions(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.prescriptions['file_name'],
        fileSeparator=','
        )
    importMicrobiologyEvents(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.microbiologyevents['file_name'],
        fileSeparator=','
        )
    importPharmacy(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.pharmacy['file_name'],
        fileSeparator=','
        )
    importProcedureEvents(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.procedureevents['file_name'],
        fileSeparator=','
        )
    importItems(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.d_items['file_name'],
        fileSeparator=','
        )
    importDatetimeEvents(
        con=con,
        destinationSchemaName=destinationSchemaName,
        filePath = Config.datetimeevents['file_name'],
        fileSeparator=','
        )
    # importChartEvents(
    #     con=con,
    #     destinationSchemaName=destinationSchemaName,
    #     filePath = Config.chartevents['file_name'],
    #     fileSeparator=','
    #     )
    # for i in ['m', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u']:
    #     filePath = '/superbugai-data/mimiciv/1.0/icu/xa'
    #     importChartEvents(
    #         con=con,
    #         destinationSchemaName=destinationSchemaName,
    #         filePath = filePath + i,
    #         fileSeparator=','
    #         )
