import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_airport_table_drop = "DROP TABLE IF EXISTS staging_airport"
staging_us_demographics_table_drop = "DROP TABLE IF EXISTS staging_us_demographics"
staging_us_immigration_table_drop = "DROP TABLE IF EXISTS staging_us_immigration"
staging_country_code_table_drop = "DROP TABLE IF EXISTS staging_country_code"
staging_visa_code_table_drop = "DROP TABLE IF EXISTS staging_visa_code"
staging_travel_code_table_drop = "DROP TABLE IF EXISTS staging_travel_code"
us_states_table_drop = "DROP TABLE IF EXISTS us_states"
us_municipality_table_drop = "DROP TABLE IF EXISTS us_municipality"
country_table_drop = "DROP TABLE IF EXISTS country"
airport_table_drop = "DROP TABLE IF EXISTS airport"
visa_code_table_drop = "DROP TABLE IF EXISTS visa_code"
us_immigration_table_drop = "DROP TABLE IF EXISTS us_immigration"
travel_code_table_drop = "DROP TABLE IF EXISTS travel_code"


# CREATE TABLES

# purpose of staging tables: increase efficiency of ETL processes, ensure data integrity and support data quality operations
# constraints should not be applied to the staging tables
staging_airport_table_create = ("""CREATE TABLE public.staging_airport(
      iata_code VARCHAR(3)
    , name VARCHAR(150)
    , iso_country VARCHAR(2)
    , iso_region VARCHAR(5)
    , municipality VARCHAR(50)
    , type VARCHAR(30)
    , state_code VARCHAR(2)
    , latitude DOUBLE PRECISION
    , longitude DOUBLE PRECISION
    );
""")

# order of columns and column names must match parquet file
staging_us_demographics_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_us_demographics (
      city VARCHAR(100)
    , state_name VARCHAR(25)
    , median_age DOUBLE PRECISION
    , male_population DOUBLE PRECISION
    , female_population DOUBLE PRECISION
    , total_population DOUBLE PRECISION
    , foreign_born DOUBLE PRECISION
    , avg_household_size DOUBLE PRECISION
    , state_code VARCHAR(4)
    );
""")

# order of columns and column names must match parquet file
staging_us_immigration_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_us_immigration (
      cicid DOUBLE PRECISION
    , year BIGINT
    , month BIGINT
    , city_code_origin BIGINT
    , country_code_residence BIGINT
    , city_code_destination VARCHAR(4)
    , arrival_date DATE
    , travel_code BIGINT
    , state_code_residence VARCHAR(4)
    , departure_date DATE
    , visa_code BIGINT
    , birth_year BIGINT
    , gender VARCHAR(4)
    , airline VARCHAR(4)
    );
""")

# order of columns and column names must match parquet file
staging_country_code_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_country_code (
      country_code BIGINT
    , country_name VARCHAR(100)
    );
""")

# order of columns and column names must match parquet file
staging_visa_code_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_visa_code (
      visa_code BIGINT
    , visa_name VARCHAR(10)
    );
""")

# order of columns and column names must match parquet file
staging_travel_code_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_travel_code (
      travel_code BIGINT
    , travel_name VARCHAR(15)
    );
""")

# If recent data is queried most frequently, specify the timestamp column as the leading column for the sort key
# fact table can have only one distribution key
us_states_table_create = ("""CREATE TABLE IF NOT EXISTS public.us_states (
      state_code VARCHAR(4) PRIMARY KEY
    , state_name VARCHAR(25)
    );
""")

us_municipality_table_create = ("""CREATE TABLE IF NOT EXISTS public.us_municipality (
      city VARCHAR(100)
    , state_code VARCHAR(4)
    , median_age DOUBLE PRECISION
    , male_population DOUBLE PRECISION
    , female_population DOUBLE PRECISION
    , total_population DOUBLE PRECISION
    , foreign_born DOUBLE PRECISION
    , avg_household_size DOUBLE PRECISION
    , PRIMARY KEY (city, state_code)
    , FOREIGN KEY (state_code) REFERENCES us_states(state_code) 
    );
""")

country_table_create = ("""CREATE TABLE IF NOT EXISTS public.country (
      country_code BIGINT PRIMARY KEY
    , country_name VARCHAR(100)
    );
""")

airport_table_create = ("""CREATE TABLE IF NOT EXISTS public.airport (
      iata_code VARCHAR(3) PRIMARY KEY
    , name VARCHAR(150)
    , iso_country VARCHAR(2)
    , iso_region VARCHAR(5)
    , municipality VARCHAR(50)
    , type VARCHAR(30)
    , state_code VARCHAR(2)
    , latitude DOUBLE PRECISION
    , longitude DOUBLE PRECISION
    , FOREIGN KEY (state_code) REFERENCES us_states(state_code) 
    );
""")

visa_code_table_create = ("""CREATE TABLE IF NOT EXISTS public.visa_code (
      visa_code BIGINT PRIMARY KEY
    , visa_name VARCHAR(10)
    );
""")

travel_code_table_create = ("""CREATE TABLE IF NOT EXISTS public.travel_code (
      travel_code BIGINT PRIMARY KEY
    , travel_name VARCHAR(15)
    );
""")

us_immigration_table_create = ("""CREATE TABLE IF NOT EXISTS public.us_immigration (
      cicid BIGINT PRIMARY KEY
    , year BIGINT
    , month BIGINT
    , city_code_origin BIGINT
    , country_code_residence BIGINT
    , city_code_destination VARCHAR(3)
    , arrival_date DATE
    , travel_code BIGINT
    , state_code_residence VARCHAR(4)
    , departure_date DATE
    , visa_code BIGINT
    , birth_year BIGINT
    , gender VARCHAR(4)
    , airline VARCHAR(4)
    , FOREIGN KEY (state_code_residence) REFERENCES us_states(state_code) 
    , FOREIGN KEY (country_code_residence) REFERENCES country(country_code) 
    , FOREIGN KEY (city_code_destination) REFERENCES airport(iata_code) 
    , FOREIGN KEY (travel_code) REFERENCES travel_code(travel_code) 
    , FOREIGN KEY (visa_code) REFERENCES visa_code(visa_code) 
    );
""")

# STAGING TABLES

# If the JSON data objects don't correspond directly to column names, you can use a JSONPaths file to map the JSON elements to columns. The order doesn't matter in the JSON source data, but the order of the JSONPaths file expressions must match the column order
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json
staging_airport_copy = ("""
                        COPY staging_airport 
                        FROM {}
                        iam_role {}
                        FORMAT AS PARQUET""").format(config['S3_redshift']['airport'],config['IAM_ROLE']['ARN'])

staging_us_demographics_copy = ("""
                        COPY staging_us_demographics 
                        FROM {}
                        iam_role {}
                        FORMAT AS PARQUET;""").format(config['S3_redshift']['us_demographics'],config['IAM_ROLE']['ARN'])

staging_us_immigration_copy = ("""
                        COPY staging_us_immigration 
                        FROM {}
                        iam_role {}
                        FORMAT AS PARQUET;""").format(config['S3_redshift']['us_immigration'],config['IAM_ROLE']['ARN'])

staging_country_code_copy = ("""
                        COPY staging_country_code
                        FROM {}
                        iam_role {}
                        FORMAT AS PARQUET;""").format(config['S3_redshift']['country_code'],config['IAM_ROLE']['ARN'])

staging_visa_code_copy = ("""
                        COPY staging_visa_code
                        FROM {}
                        iam_role {}
                        FORMAT AS PARQUET;""").format(config['S3_redshift']['visa_code'],config['IAM_ROLE']['ARN'])

staging_travel_code_copy = ("""
                        COPY staging_travel_code
                        FROM {}
                        iam_role {}
                        FORMAT AS PARQUET;""").format(config['S3_redshift']['travel_code'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
# UNIQUE is not enforced on tables by Amazon Redshift, hence ensure unique values with select distinct
us_states_table_insert = ("""
    INSERT INTO us_states( 
                              state_code
                            , state_name
                         )
    SELECT distinct d.state_code
         , d.state_name
    FROM staging_us_demographics d
    WHERE d.state_code IS NOT NULL 
""")

us_municipality_table_insert = ("""
    INSERT INTO us_municipality (
                              city
                            , state_code
                            , median_age
                            , male_population
                            , female_population
                            , total_population
                            , foreign_born
                            , avg_household_size
                        )
    SELECT distinct   d.city
                    , d.state_code
                    , d.median_age
                    , d.male_population
                    , d.female_population
                    , d.total_population
                    , d.foreign_born
                    , d.avg_household_size
    FROM staging_us_demographics d
    WHERE d.city IS NOT NULL    
""")

country_table_insert = ("""
    INSERT INTO country (
                          country_code
                        , country_name
                      )
    SELECT distinct   c.country_code
                    , c.country_name
    FROM staging_country_code c
    WHERE c.country_code IS NOT NULL
""")

airport_table_insert = ("""
    INSERT INTO airport (
                          iata_code
                        , name
                        , iso_country
                        , iso_region
                        , municipality
                        , type
                        , state_code
                        , latitude
                        , longitude
                        )
    SELECT distinct   a.iata_code
                    , a.name
                    , a.iso_country
                    , a.iso_region
                    , a.municipality
                    , a.type
                    , a.state_code
                    , a.latitude
                    , a.longitude
    FROM staging_airport a
    WHERE a.iata_code IS NOT NULL
""")

visa_code_table_insert = ("""
    INSERT INTO visa_code (
                      visa_code
                    , visa_name
                    )
     SELECT distinct  v.visa_code
                    , v.visa_name
     FROM staging_visa_code v
     WHERE v.visa_code IS NOT NULL
""")

us_immigration_table_insert = ("""
    INSERT INTO us_immigration (
                      cicid
                    , month
                    , city_code_origin
                    , country_code_residence
                    , city_code_destination
                    , arrival_date
                    , travel_code
                    , state_code_residence
                    , departure_date
                    , visa_code
                    , birth_year
                    , gender
                    , airline
                    )
     SELECT distinct  i.cicid
                    , i.month
                    , i.city_code_origin
                    , i.country_code_residence
                    , i.city_code_destination
                    , i.arrival_date
                    , i.travel_code
                    , i.state_code_residence
                    , i.departure_date
                    , i.visa_code
                    , i.birth_year
                    , i.gender
                    , i.airline
     FROM staging_us_immigration i
     WHERE i.cicid IS NOT NULL
""")

travel_code_table_insert = ("""
    INSERT INTO travel_code (
                      travel_code
                    , travel_name
                    )
     SELECT distinct  t.travel_code
                    , t.travel_name
     FROM staging_travel_code t
     WHERE t.travel_code IS NOT NULL
""")
                            
#data qulity ckeck: count in tables
us_immigration_count_check = '''SELECT COUNT(*) FROM us_immigration'''
us_states_count_check = '''SELECT COUNT(*) FROM us_states'''
us_municipality_count_check = '''SELECT COUNT(*) FROM us_municipality'''
country_count_check = '''SELECT COUNT(*) FROM country'''
airport_count_check = '''SELECT COUNT(*) FROM airport'''
visa_code_count_check = '''SELECT COUNT(*) FROM visa_code'''
travel_code_count_check = '''SELECT COUNT(*) FROM travel_code'''
 
#data qulity ckeck: null reference in foreign key
#references us_states
us_immigration_state_code_residence_null_reference_check = '''SELECT COUNT(*) FROM us_immigration WHERE state_code_residence is null'''
#references airport
us_immigration_city_code_destination_null_reference_check = '''SELECT COUNT(*) FROM us_immigration WHERE city_code_destination is null'''
#references country
us_immigration_country_code_residence_null_reference_check = '''SELECT COUNT(*) FROM us_immigration WHERE country_code_residence is null'''
#references travel_code
us_immigration_travel_code_null_reference_check = '''SELECT COUNT(*) FROM us_immigration WHERE travel_code is null'''
#references visa_code
us_immigration_visa_code_null_reference_check = '''SELECT COUNT(*) FROM us_immigration WHERE visa_code is null'''
#references state_code
airport_state_code_null_reference_check = '''SELECT COUNT(*) FROM airport WHERE state_code is null'''
                            
# QUERY LISTS
#create us_immigration last since it contains references to other tables
create_table_queries = [staging_airport_table_create, staging_us_demographics_table_create, staging_us_immigration_table_create, staging_country_code_table_create,staging_visa_code_table_create, staging_travel_code_table_create, us_states_table_create, us_municipality_table_create, country_table_create, airport_table_create, visa_code_table_create, travel_code_table_create, us_immigration_table_create]

#drop us_immigration first since it contains references to other tables
drop_table_queries = [staging_airport_table_drop, staging_us_demographics_table_drop, staging_us_immigration_table_drop, staging_country_code_table_drop, staging_visa_code_table_drop, staging_travel_code_table_drop, us_immigration_table_drop, us_municipality_table_drop, country_table_drop, airport_table_drop, visa_code_table_drop, travel_code_table_drop, us_states_table_drop]
                            
copy_table_queries = [staging_airport_copy, staging_us_demographics_copy, staging_country_code_copy, staging_visa_code_copy, staging_travel_code_copy, staging_us_immigration_copy]

#insert us_immigration last since it contains references to other tables  
insert_table_queries = [us_states_table_insert, us_municipality_table_insert, country_table_insert, airport_table_insert, visa_code_table_insert, travel_code_table_insert, us_immigration_table_insert]

#ensure that load is complete
data_quality_count_check = [us_immigration_count_check, us_states_count_check, us_municipality_count_check, country_count_check, airport_count_check, visa_code_count_check, travel_code_count_check]
                            
#data quality check if there are null references
data_quality_null_references_check = [us_immigration_state_code_residence_null_reference_check, us_immigration_city_code_destination_null_reference_check, us_immigration_country_code_residence_null_reference_check, us_immigration_travel_code_null_reference_check, us_immigration_visa_code_null_reference_check, airport_state_code_null_reference_check]