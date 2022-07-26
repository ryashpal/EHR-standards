import logging
import sys

log = logging.getLogger("Standardise")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

import Lookup
import Import
import Stage
from etl import Location
from etl import Person
from etl import Death
from etl import CareSite
from etl import Visit
from etl import Measurement
from etl import Diagnoses
from etl import Procedure
from etl import Observation
from etl import ConditionOccurrence
from etl import Specimen
from etl import Drug
from etl import DeviceExposure
from etl import Relationship
from etl import Source
import Unload

import Config


def getConnnection():

    log.info("Establishing DB connection")
    import psycopg2

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(
        dbname=Config.sql_db_name,
        user=Config.sql_user_name,
        host=Config.sql_host_name,
        port=Config.sql_port_number,
        password=Config.sql_password
        )

    log.info("DB connection obtained successfully")

    return con


def dropSchema(con, schemaName):
    log.info("Dropping schema: " + schemaName)
    dropSchemaQuery = """drop schema if exists """ + schemaName + """ cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropSchemaQuery)


def createSchema(con, schemaName):
    log.info("Creating schema: " + schemaName)
    createSchemaQuery = """create schema if not exists """ + schemaName
    with con:
        with con.cursor() as cursor:
            cursor.execute(createSchemaQuery)


def createLookupTables(con):
    Lookup.migrate(con=con, destinationSchemaName=Config.schema_name)


def importFromCsv(con):
    Import.importCsv(con=con, destinationSchemaName=Config.schema_name)


def stageData(con):
    Stage.migrate(con=con, sourceSchemaName=Config.schema_name, destinationSchemaName=Config.schema_name)


def performETL(con):
    # # Run the following code in the given order

    # # 01. Location
    # Location.migrate(con=con, schemaName=Config.schema_name)

    # # 02. Person
    # Person.migrate(con=con, schemaName=Config.schema_name)

    # # 03. Death
    # Death.migrate(con=con, schemaName=Config.schema_name)

    # # 04. Care Site
    # CareSite.migrate(con=con, schemaName=Config.schema_name)

    # # 05. Visit Occurrence Part 1
    # Visit.migratePart1(con=con, schemaName=Config.schema_name)

    # # 06. Measurement Units
    # Measurement.migrateUnits(con=con, schemaName=Config.schema_name)

    # # 07. Measurement Chartevents
    # Measurement.migrateChartevents(con=con, schemaName=Config.schema_name)

    # # 08. Measurement Labevents
    # Measurement.migrateLabevents(con=con, schemaName=Config.schema_name)

    # # 09. Measurement Specimen
    # Measurement.migrateSpecimen(con=con, schemaName=Config.schema_name)

    # # 10. Visit Occurrence Part 2
    # Visit.migratePart2(con=con, schemaName=Config.schema_name)

    # # 11. Visit Occurrence
    # Visit.migrateVisitOccurrence(con=con, schemaName=Config.schema_name)

    # # 12. Visit Detail
    # Visit.migrateVisitDetail(con=con, schemaName=Config.schema_name)

    # # 13. Diagnosis
    # Diagnoses.migrate(con=con, schemaName=Config.schema_name)

    # # 14. Procedure Lookup
    # Procedure.migrateLookup(con=con, schemaName=Config.schema_name)

    # # 15. Observation Lookup
    # Observation.migrateLookup(con=con, schemaName=Config.schema_name)

    # # 16. Condition Occurrence
    # ConditionOccurrence.migrate(con=con, schemaName=Config.schema_name)

    # # 17. Procedure occurrence
    # Procedure.migrate(con=con, schemaName=Config.schema_name)

    # # 18. Specimen
    # Specimen.migrate(con=con, schemaName=Config.schema_name)

    # # 19. Measurement
    # Measurement.migrate(con=con, schemaName=Config.schema_name)

    # # 20. Drug Lookup
    # Drug.migrateLookup(con=con, schemaName=Config.schema_name)

    # # 21. Drug Exposure
    # Drug.migrate(con=con, schemaName=Config.schema_name)

    # # 22. Device Exposure
    # DeviceExposure.migrate(con=con, schemaName=Config.schema_name)

    # # 23. Observation
    # Observation.migrate(con=con, schemaName=Config.schema_name)

    # # 24. Observation Period
    # Observation.migratePeriod(con=con, schemaName=Config.schema_name)

    # # 25. Person Final
    # Person.migrateFinal(con=con, schemaName=Config.schema_name)

    # # 26. Fact Relationship
    # Relationship.migrate(con=con, schemaName=Config.schema_name)

    # # 27. Condition Era
    # ConditionOccurrence.migrateConditionEra(con=con, schemaName=Config.schema_name)

    # # 28. Drug Era
    # Drug.migrateDrugEra(con=con, schemaName=Config.schema_name)

    # # 29. Dose Era
    # Drug.migrateDoseEra(con=con, schemaName=Config.schema_name)

    # 30. 
    # Not migrated

    # 31. Source
    Source.migrate(con=con, schemaName=Config.schema_name)


def unloadData(con):
    # 98. Unload Vocabulary
    Unload.unloadVocabulary(con=con, sourceSchemaName=Config.schema_name, destinationSchemaName=Config.schema_name)

    # 99. Unload data
    Unload.unloadData(con=con, sourceSchemaName=Config.schema_name, destinationSchemaName=Config.schema_name)


if __name__ == "__main__":

    log.info("Start")

    con = getConnnection()

    # dropSchema(con=con, schemaName=Config.schema_name)
    # createSchema(con=con, schemaName=Config.schema_name)

    # createLookupTables(con=con)

    # importFromCsv(con=con)

    # stageData(con=con)

    performETL(con=con)

    # unloadData(con=con)

    log.info("End")
