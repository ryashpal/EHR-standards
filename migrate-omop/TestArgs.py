import argparse

parser = argparse.ArgumentParser(description='Migrate EHR to OMOP-CDM')
parser.add_argument('-l', '--create_lookup', action='store_false',
                    help='Create lookup by importing Athena vocabulary and custom mapping')
parser.add_argument('-f', '--import_file', action='store_false',
                    help='Import EHR from a csv files')
parser.add_argument('-e', '--perform_etl', action='store_false',
                    help='Perform migration Extract-Transform-Load (ETL) operations')


args = parser.parse_args()
print(args)
