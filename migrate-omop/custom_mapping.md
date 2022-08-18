# Instructions to create custom mapping

Creating custom mapping involves the following steps;

1. Identify an attribute that needs to be mapped to standard vocabulary and get all its unique values
2. Identify corresponding standard concept to every value
3. Add a row in `custom mapping` csv file containing both the non-standard value and the standard concept to be mapped
4. Import `custom mapping` file

Here is an example given below issustrating step-by-step procedure to be followed to create a mapping entry for one of the attribute. in this `Drug Route`;

1. Get all the unique `Drug Route`s from the source. I am running the following query to get the unique values.

```sql
select distinct(route) from pharmacy;
```

2. The previous step results in 100 unique `Drug Route`s. We take one value from the list at a time ex. `IV DRIP` and try to identify corresponding standard concept. For that we ran the following query and select an appropriate standard concept from the result.

```sql
select * from concept where upper(domain_id) like '%ROUTE%' and upper(concept_name) like 'I%V%';
```

3. Based on the identified value from we create a csv line that will to be added to the `custom mapping` file. This line links the *IV DRIP* value in the `Drug Route` attribute to the standard SNOMED concept i.e. Intravenous route with the concept id 4171047.

```
IV DRIP,2000002005,mimiciv_drug_route,Route,Qualifier Value,,IV DRIP,1970-01-01,2099-12-31,,4171047,Maps to,Mapped from,1970-01-01,2099-12-31,
```

4. We import this `custom mapping` file to the reference vocabulary for the EHR. More details on procedure to import custom mapping can be found in the readme file of this reposotory.