import logging

log = logging.getLogger("Standardise")


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
    import psycopg2.extras

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.dod.replace({np.nan: None}, inplace=True)
    if len(df) > 0:
        table = destinationSchemaName + '.PATIENTS'
        df_columns = ['subject_id', 'gender', 'anchor_age', 'anchor_year', 'anchor_year_group', 'dod']
        columns = '"' + '", "'.join(df_columns) + '"'
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        cur = con.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df[df_columns].values)
        con.commit()
        cur.close()


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
    import psycopg2.extras

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.deathtime.replace({np.nan: None}, inplace=True)
    df.edregtime.replace({np.nan: None}, inplace=True)
    df.edouttime.replace({np.nan: None}, inplace=True)
    if len(df) > 0:
        table = destinationSchemaName + '.ADMISSIONS'
        df_columns = ['subject_id', 'hadm_id', 'admittime', 'dischtime', 'deathtime', 'admission_type', 'admission_location', 'discharge_location', 'insurance', 'language', 'marital_status', 'ethnicity', 'edregtime', 'edouttime', 'hospital_expire_flag']
        columns = '"' + '", "'.join(df_columns) + '"'
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        cur = con.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df[df_columns].values)
        con.commit()
        cur.close()


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
    import psycopg2.extras

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.intime.replace({np.nan: None}, inplace=True)
    df.outtime.replace({np.nan: None}, inplace=True)
    if len(df) > 0:
        table = destinationSchemaName + '.TRANSFERS'
        df_columns = ['subject_id', 'hadm_id', 'transfer_id', 'eventtype', 'careunit', 'intime', 'outtime']
        columns = '"' + '", "'.join(df_columns) + '"'
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        cur = con.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df[df_columns].values)
        con.commit()
        cur.close()


def importDiagnoses(con, sourceSchemaName, destinationSchemaName):
    pass


def importServices(con, sourceSchemaName, destinationSchemaName):
    pass


def importLabEvents(con, sourceSchemaName, destinationSchemaName):
    pass


def importLabItems(con, sourceSchemaName, destinationSchemaName):
    pass


def importProcedures(con, sourceSchemaName, destinationSchemaName):
    pass


def importHcpcsEvents(con, sourceSchemaName, destinationSchemaName):
    pass


def importDrugCodes(con, sourceSchemaName, destinationSchemaName):
    pass


def importPrescriptions(con, sourceSchemaName, destinationSchemaName):
    pass


def importMicrobiologyEvents(con, sourceSchemaName, destinationSchemaName):
    pass


def importMicrobiologyItems(con, sourceSchemaName, destinationSchemaName):
    pass


def importPharmacy(con, sourceSchemaName, destinationSchemaName):
    pass


def importProcedureEvents(con, sourceSchemaName, destinationSchemaName):
    pass


def importItems(con, sourceSchemaName, destinationSchemaName):
    pass


def importDatetimeEvents(con, sourceSchemaName, destinationSchemaName):
    pass


def importChartEvents(con, sourceSchemaName, destinationSchemaName):
    pass


def import_csv(con, sourceSchemaName, destinationSchemaName):
    importPatients(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importAdmissions(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importTransfers(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importDiagnoses(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importServices(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importLabEvents(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importLabItems(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importProcedures(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importHcpcsEvents(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importDrugCodes(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importPrescriptions(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importMicrobiologyEvents(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importMicrobiologyItems(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importPharmacy(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importProcedureEvents(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importItems(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importDatetimeEvents(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    importChartEvents(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
