# CREATE TABLES
create_staging_airport_codes = ("""
CREATE TABLE staging_airport_codes (
    ident VARCHAR, 
    type VARCHAR, 
    name VARCHAR, 
    elevation_ft VARCHAR, 
    continent VARCHAR, 
    iso_country VARCHAR,
    iso_region VARCHAR, 
    municipality VARCHAR, 
    gps_code VARCHAR, 
    iata_code VARCHAR, 
    local_code VARCHAR,
    coordinates VARCHAR
)
""")

create_staging_imigration = ("""CREATE TABLE staging_imigration (
    Unnamed_0 INTEGER, 
    cicid FLOAT, 
    i94yr FLOAT, 
    i94mon FLOAT, 
    i94cit FLOAT, 
    i94res FLOAT, 
    i94port VARCHAR,
    arrdate FLOAT, 
    i94mode FLOAT, 
    i94addr VARCHAR, 
    depdate FLOAT, 
    i94bir FLOAT, 
    i94visa FLOAT,
    count FLOAT, 
    dtadfile VARCHAR, 
    visapost VARCHAR, 
    occup VARCHAR, 
    entdepa VARCHAR, 
    entdepd VARCHAR,
    entdepu VARCHAR, 
    matflag VARCHAR, 
    biryear FLOAT, 
    dtaddto VARCHAR, 
    gender VARCHAR, 
    insnum INTEGER,
    airline VARCHAR, 
    admnum FLOAT, 
    fltno VARCHAR, 
    visatype VARCHAR
)""")

create_staging_sas_information = ("""CREATE TABLE staging_sas_information (
    id	VARCHAR,
    description	VARCHAR,
    column_name VARCHAR 
)""")

create_dim_country = ("""CREATE TABLE dim_country (
    id VARCHAR,
    country VARCHAR
)""")

create_dim_state = ("""CREATE TABLE dim_state (
    id VARCHAR,
    state VARCHAR
)""")

create_dim_modal = ("""CREATE TABLE dim_modal (
    id_modal VARCHAR,
    modal VARCHAR
)""")

create_dim_visa_motive = ("""CREATE TABLE dim_visa_motive (
    visa_motive_id VARCHAR,
    motive VARCHAR
)""")

create_dim_port = ("""CREATE TABLE dim_port (
    port_id VARCHAR,
    modal_id VARCHAR,
    port_type VARCHAR,
    port_name VARCHAR,
    port_country VARCHAR,
    port_city VARCHAR
)""")

create_dim_imigrant = ("""CREATE TABLE dim_imigrant(
    imigrant_id VARCHAR,
    gender VARCHAR,
    occupation VARCHAR,
    birth_year INTEGER,
    admission_number VARCHAR,
    birth_country_id INTEGER,
    state_residence_id VARCHAR,
    visa_motive_id INTEGER,
    visa_issued_place VARCHAR,
    visa_type VARCHAR
)""")
                       
create_fact_imigration = ("""
CREATE TABLE fact_imigration (
    id INT IDENTITY(1,1),
    imigrant_id VARCHAR,
    id_port_arrival_us  VARCHAR,
    count INTEGER,
    flight_number VARCHAR
)
""")

# LOAD STAGE TABLES
load_staging_tables = ("""
    COPY {} FROM '{}'
    credentials 'aws_iam_role=arn:aws:iam::644393144861:role/imigrationRole'
    region '{}'
    delimiter '{}' EMPTYASNULL CSV NULL AS '\\0'
    IGNOREHEADER 1;
""")
                     
# LOAD TABLES

load_dim_country = ("""
SELECT
    id,
    CASE
        WHEN description = 'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)' THEN 'MEXICO' 
        WHEN description LIKE 'No Country Code %' THEN 'UNKNOWN'
        WHEN description LIKE 'INVALID%' THEN 'UNKNOWN'
        ELSE description
    END AS country
FROM staging_sas_information
WHERE column_name = 'i94cit_res'
ORDER BY 1 ASC
""")

load_dim_state = ("""
SELECT
    id,
    description
FROM staging_sas_information
WHERE column_name = 'i94addr'
    AND id NOT IN ('99', 'GU', 'PR', 'VI')
ORDER BY 1 ASC
""")

load_dim_modal = ("""
SELECT
    id AS id_modal,
    description AS modal
FROM staging_sas_information
WHERE column_name = 'i94mode'
ORDER BY 1 ASC
""")

load_dim_visa_motive = ("""
SELECT
    id AS visa_motive_id,
    description AS motive
FROM staging_sas_information
WHERE column_name = 'i94visa'
ORDER BY 1 ASC
""")

load_dim_port = ("""
SELECT DISTINCT
    si.i94port AS port_id,
    si.i94mode AS modal_id,
    CASE 
        WHEN dm.modal = 'Air' AND sac.type IS NOT NULL THEN sac.type 
        WHEN sac.type IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN'
    END AS port_type,
    CASE 
        WHEN dm.modal = 'Air' AND sac.name IS NOT NULL THEN sac.name 
        WHEN sac.name IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN'
    END AS port_name,
    CASE 
        WHEN dm.modal = 'Air' AND sac.iso_country IS NOT NULL THEN sac.iso_country 
        WHEN sac.iso_country IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN'
    END AS port_country,
    CASE 
        WHEN dm.modal = 'Air' AND sac.municipality IS NOT NULL THEN sac.municipality 
        WHEN sac.municipality IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN'
    END AS port_city
FROM staging_imigration si
LEFT JOIN staging_airport_codes sac ON si.i94port = sac.iata_code
LEFT JOIN dim_modal dm ON dm.id_modal = si.i94mode
""")

load_dim_imigrant = ("""
    SELECT
        cicid AS imigrant_id,
        CASE
            WHEN gender = 'X' OR gender IS NULL THEN 'UNKNOWN' 
            ELSE gender 
        END AS gender,
        CASE
            WHEN occup IS NULL THEN 'UNKNOWN'
            ELSE occup
        END AS occupation,
        biryear AS birth_year,
        admnum AS admission_number,
        i94cit AS birth_country_id,
        i94addr AS state_residence_id,
        i94visa AS visa_motive_id,
        CASE
            WHEN visapost IS NULL THEN 'UNKNOWN'
            ELSE visapost
        END AS visa_issued_place,
        visatype AS visa_type
    FROM staging_imigration
""")

load_fact_imigration = ("""
SELECT
    cicid AS imigrant_id,
    i94port AS id_port_arrival_us,
    count,
    fltno AS flight_number
FROM staging_imigration
""")