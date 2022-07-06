# database connection details

sql_host_name = 'localhost'
sql_port_number = 5434
sql_user_name = 'postgres'
sql_password = 'mysecretpassword'
sql_db_name = 'mimic4'

# new schema to host the migrated tables

schema_name = 'omop_migration_test'

file_names = {
    'patients': '/superbugai-data/mimiciv/1.0/core/patients.csv',
    'admission': '/superbugai-data/mimiciv/1.0/core/admissions.csv',
    'transfers': '/superbugai-data/mimiciv/1.0/core/transfers.csv',
}
