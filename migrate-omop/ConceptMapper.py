import logging

log = logging.getLogger("Standardise")


def createCustomMapping(
    con
    , etlSchemaName: str
    , fieldName: str
    , tableName: str
    , whereCondition: str
    , sourceVocabularyId: str
    , domainId: str
    , vocabularyId: str
    , conceptClassId: str
    , keyPhrase: str
    ):

    import pandas as pd
    import re
    import datetime

    sourceConceptsDf = pd.read_sql_query(
        """select distinct(""" + fieldName + """) from """ + etlSchemaName + """.""" + tableName + """ """ + whereCondition
        , con
        )

    standardConceptsQuery = """
    select
    *
    from
    """ + etlSchemaName + """.voc_concept
    where
    domain_id = '""" + domainId + """'
    and vocabulary_id = '""" + vocabularyId + """'
    and concept_class_id = '""" + conceptClassId + """'
    ;
    """

    standardConceptsDf = pd.read_sql_query(standardConceptsQuery, con)

    # fuzz is used to compare TWO strings
    from fuzzywuzzy import fuzz

    # process is used to compare a string to MULTIPLE other strings
    from fuzzywuzzy import process

    df = None
    for index, row in sourceConceptsDf.iterrows():
        matchingConcept = process.extract(re.sub(r'[\$\{\(\[\^\}\]\+\)\/]+', ' ', row[fieldName]) + keyPhrase, standardConceptsDf['concept_name'], limit=1, scorer=fuzz.token_set_ratio)
        standardRow = standardConceptsDf[standardConceptsDf['concept_name'] == matchingConcept[0][0]].head(1)
        standardRow['concept_name'] = row[fieldName]
        currentConceptIdQuery = """select max(source_concept_id) from """ + etlSchemaName + """.tmp_custom_mapping where source_concept_id > 2100000000"""
        currentConceptIdDf = pd.read_sql_query(currentConceptIdQuery, con)
        standardRow['source_concept_id'] = 1 + max(2100000000, 0 if (currentConceptIdDf['max'][0] is None) else currentConceptIdDf['max'][0], (0 if (df is None) else max(df['source_concept_id'].values)))
        standardRow['source_vocabulary_id'] = sourceVocabularyId
        standardRow['source_domain_id'] = standardRow['domain_id']
        standardRow['source_concept_class_id'] = standardRow['concept_class_id']
        standardRow['standard_concept'] = None
        standardRow['concept_code'] = row[fieldName]
        standardRow['valid_start_date'] = datetime.datetime(1970, 1, 1)
        standardRow['valid_end_date'] = datetime.datetime(2099, 12, 31)
        standardRow['invalid_reason'] = None
        standardRow['relationship_id'] = 'Maps to'
        standardRow['reverese_relationship_id'] = 'Mapped from'
        standardRow['invalid_reason_cr'] = None
        standardRow['relationship_valid_start_date'] = datetime.datetime(1970, 1, 1)
        standardRow['relationship_end_date'] = datetime.datetime(2099, 12, 31)
        if df is None:
            df = standardRow
        else:
            df = pd.concat([df, standardRow], axis=0, ignore_index=True)

    df.rename(
        columns={
            'concept_id': 'target_concept_id'
            }
        , inplace=True
        )
    return df
