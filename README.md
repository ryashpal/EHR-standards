# EHR-standards

## How to run

Use Run.py to execute the migration scripts

This will perform the sollowing steps;

1. Create Lookup tables
2. Import data from CSV (if needed)
3. Stage data for migration
4. Perform ETL
5. Unload data in to final tables

Steps that are not necessary can be commented out

## Configuration

The configuration file contain the following configurations;

### Database connection details

sql_host_name: Host name of the database
sql_port_number: Port number of the database
sql_user_name: User name of the database
sql_password: Password of the database
sql_db_name: Databse name of the database

schema_name: New schema to host the migrated tables

### CSV file details and mapping

Ex:

patients = {
    'file_name': Path for the csv file
    'column_mapping': {
        -- CSV column name: Standard column name,
    },
}
