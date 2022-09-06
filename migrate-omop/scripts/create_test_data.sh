######################################################################################################################################
#######################################################      CORE     ################################################################
######################################################################################################################################

# For creating patients.csv
cat 1.0/core/patients.csv | awk 'BEGIN {srand()} !/^$/ { if (rand() <= .01) print $0}' > test_data/patients.csv

# For creating admissions.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/core/admissions.csv)  > test_data/admissions.csv

# For creating transfers.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/core/transfers.csv) > test_data/transfers.csv


######################################################################################################################################
#######################################################      HOSP     ################################################################
######################################################################################################################################

# For creating diagnoses_icd.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/hosp/diagnoses_icd.csv) > test_data/diagnoses_icd.csv

# For creating services.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/hosp/services.csv) > test_data/services.csv

# For creating labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xaa) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xab) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xac) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xad) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xae) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xaf) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xag) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xah) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xai) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xaj) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xak) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xal) >> test_data/labevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/xam) >> test_data/labevents.csv

# For creating d_labitems.csv
cp 1.0/hosp/d_labitems.csv test_data/d_labitems.csv

# For creating procedures_icd.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/hosp/procedures_icd.csv) > test_data/procedures_icd.csv

# For creating hcpcsevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/hosp/hcpcsevents.csv) > test_data/hcpcsevents.csv

# For creating drgcodes.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/hosp/drgcodes.csv) > test_data/drgcodes.csv

# For creating prescriptions.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15,2.16,2.17 <(sort -k1 test_data/patients.csv) <(sort -k1 1.0/hosp/prescriptions.csv) > test_data/prescriptions.csv

# For creating microbiologyevents.csv
join -t , -1 1 -2 2 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15,2.16,2.17,2.18,2.19,2.20,2.21,2.22,2.23,2.24 <(sort -k1 -t , test_data/patients.csv) <(sort -k2 -t , 1.0/hosp/microbiologyevents.csv) > test_data/microbiologyevents.csv

# For creating pharmacy.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15,2.16,2.17,2.18,2.19,2.20,2.21,2.22,2.23,2.24,2.25,2.26,2.27 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/hosp/pharmacy.csv) > test_data/pharmacy.csv

# For creating procedureevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,2.11,2.12,2.13,2.14,2.15,2.16,2.17,2.18,2.19,2.20,2.21,2.22,2.23,2.24,2.25,2.26 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/procedureevents.csv) > test_data/procedureevents.csv

# For creating d_items.csv
cp 1.0/icu/d_items.csv test_data/d_items.csv

# For creating datetimeevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/datetimeevents.csv) > test_data/datetimeevents.csv

# For creating chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xaa) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xab) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xac) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xad) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xae) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xaf) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xag) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xah) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xai) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xaj) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xak) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xal) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xam) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xan) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xao) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xap) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xaq) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xar) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xas) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xat) >> test_data/chartevents.csv
join -t , -j 1 -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10 <(sort -k1 -t , test_data/patients.csv) <(sort -k1 -t , 1.0/icu/xau) >> test_data/chartevents.csv
