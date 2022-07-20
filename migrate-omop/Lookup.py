import logging

log = logging.getLogger("Standardise")


def migrate(con, schemaName):
    log.info("Creating table: " + schemaName + ".cdm_cdm_source")
