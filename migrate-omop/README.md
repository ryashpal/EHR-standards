# Migrate OMOP-CDM


## Features

This library can be used to perform the following operations;

1. Import Athena vocabulary
2. Create custom lookup tables
3. Import EHR data from CSV files
4. Stage data for migration
4. Perform ETL
5. Unload data in to CDM schema


## Install

* Clone this [repository](https://github.com/ryashpal/EHR-standards)
```bash
git clone https://github.com/ryashpal/EHR-standards.git
```
* Create a Python virtual environment (3.8 or higher)
```bash
python -m venv .venv
```
* Install the dependencies
```bash
pip install -r requirements.txt
```


## Configuration

The configuration file contain the following configurations;


1. Database connection details

*Connection details of the database*

```bash
sql_host_name: Host name of the database

sql_port_number: Port number of the database

sql_user_name: User name of the database

sql_password: Password of the database

sql_db_name: Databse name of the database
```


2. New schema names

*Schema names to create and host the migrated tables*

```bash
lookup_schema_name: New schema name to host the vocabulary tables

etl_schema_name: New schema name to host the temporary migration tables

cdm_schema_name: New schema name to host the CDM tables
```


3. Vocabulary files path

*File paths of the Athena vocabulary files and the custom mapping*

```bash
vocabulary = {
    'concept': '/path/to/CONCEPT.csv',
    'vocabulary': '/path/to/VOCABULARY.csv',
    'domain': '/path/to/DOMAIN.csv',
    'concept_class': '/path/to/CONCEPT_CLASS.csv',
    'concept_relationship': '/path/to/CONCEPT_RELATIONSHIP.csv',
    'relationship': '/path/to/RELATIONSHIP.csv',
    'concept_synonym': '/path/to/CONCEPT_SYNONYM.csv',
    'concept_ancestor': '/path/to/CONCEPT_ANCESTOR.csv',
    'tmp_custom_mapping': '/path/to/tmp_custom_mapping.csv',
}
```


4. CSV file column mapping

*CSV file paths containing EHR data and the column mappings*

Ex:

```bash
patients = {

    file_name: Path for the csv file
    
    column_mapping: {
    
        -- column name in the file: standard column name,
        
    },
    
}
```


## Run

1. To select the virtual environment
```bash
source .venv/bin/activate
```

2. To create lookup by importing Athena vocabulary and custom mapping
```bash
python Run.py -l
```
or
```bash
python Run.py --create_lookup
```

3. To import EHR from a csv files
```bash
python Run.py -f
```
or
```bash
python Run.py --import_file
```

4. To perform migration Extract-Transform-Load (ETL) operations
```bash
python Run.py -e
```
or
```bash
python Run.py --perform_etl
```

5. To unload data to CDM schema
```bash
python Run.py -u
```
or
```bash
python Run.py --unload
```

6. To view the help menu
```bash
python Run.py  -h
```
Output
```bash
2022-08-18 11:21:49,380 - Standardise - INFO - Parsing command line arguments
usage: Run.py [-h] [-l] [-f] [-e] [-u]

Migrate EHR to OMOP-CDM

optional arguments:
  -h, --help           show this help message and exit
  -l, --create_lookup  Create lookup by importing Athena vocabulary and custom mapping
  -f, --import_file    Import EHR from a csv files
  -e, --perform_etl    Perform migration Extract-Transform-Load (ETL) operations
  -u, --unload         Unload data to CDM schema
```
