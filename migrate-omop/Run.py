import logging
import sys

log = logging.getLogger("Standardise")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

import Import
import Stage
import Location
import Person
import Death
import CareSite
import Visit
import Measurement

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


if __name__ == "__main__":

    log.info("Start")

    con = getConnnection()

    # dropSchema(con=con, schemaName=Config.schema_name)
    # createSchema(con=con, schemaName=Config.schema_name)

    # # -1. Import
    # Import.importPatients(
    #     con=con,
    #     destinationSchemaName=Config.schema_name,
    #     filePath = Config.file_names['patients'],
    #     fileSeparator=','
    #     )
    # Import.importAdmissions(
    #     con=con,
    #     destinationSchemaName=Config.schema_name,
    #     filePath = Config.file_names['admission'],
    #     fileSeparator=','
    #     )
    Import.importTransfers(
        con=con,
        destinationSchemaName=Config.schema_name,
        filePath = Config.file_names['transfers'],
        fileSeparator=','
        )

    # # 00. Stage
    # Stage.migrate(con=con, schemaName=Config.schema_name)

    # # Run the following code without mixing up the order

    # # 01. Location
    # Location.migrate(con=con, schemaName=Config.schema_name)

    # # 02. Person
    # Person.migrate(con=con, schemaName=Config.schema_name)

    # # 03. Death
    # Death.migrate(con=con, schemaName=Config.schema_name)

    # 04. Care Site
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

    log.info("End")
