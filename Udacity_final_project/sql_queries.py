# CREATE TABLES

create_staging_imigration = ("""CREATE TABLE staging_imigration (
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
    insnum VARCHAR,
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
    port_modal_id VARCHAR,
    port_id VARCHAR,
    modal_id VARCHAR,
    location_city VARCHAR,
    location_state_id VARCHAR
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
    port_modal_id VARCHAR,
    imigrant_id VARCHAR,
    id_port_arrival_us  VARCHAR,
    id_modal VARCHAR,
    count INTEGER,
    flight_number VARCHAR
)
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
    CAST(si.i94port AS VARCHAR) + '_' + CAST(si.i94mode AS VARCHAR) AS port_modal_id,
    si.i94port AS port_id,
    si.i94mode AS modal_id,
    SPLIT_PART(sas.description, ',', 1) AS location_city,
    SPLIT_PART(sas.description, ',', 2) AS location_state_id
FROM staging_imigration si
LEFT JOIN staging_sas_information sas ON si.i94port = sas.id AND column_name = 'i94port'
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
    CAST(i94port AS VARCHAR) + '_' + CAST(i94mode AS VARCHAR) AS port_modal_id,    
    cicid AS imigrant_id,
    i94port AS id_port_arrival_us,
    i94mode AS id_modal,
    CAST(count AS INTEGER) AS count,
    fltno AS flight_number
FROM staging_imigration
""")

# ANALYTIC QUERIES

# Total of imigrants in April 2016
total_imigration = ("""
SELECT
    SUM(count) AS total_imigrants
FROM fact_imigration
""")

# Imigrations by transport modal
imigrant_modal_transp = ("""
SELECT
    dm.modal,
    COUNT(fi.count) AS count_modal_arrivals
FROM fact_imigration fi
LEFT JOIN dim_port dp ON fi.port_modal_id = dp.port_modal_id
LEFT JOIN dim_modal dm ON dm.id_modal = dp.modal_id
GROUP BY 1
ORDER BY 2 DESC
""")

# What is the most common visa type for aerial arrivals
visa_type_aerial = ("""
SELECT
    dvm.motive,
    SUM(count) AS most_comom_visa_motive
FROM fact_imigration fi
LEFT JOIN dim_imigrant di ON di.imigrant_id = fi.imigrant_id
LEFT JOIN dim_visa_motive dvm ON dvm.visa_motive_id = di.visa_motive_id
WHERE fi.id_modal = 1
GROUP BY 1
ORDER BY 2 DESC
""")

# Which is the most common place where people arrive using aerial modal and comes for pleasure?
arrival_aerials_pleasure = ("""
SELECT
    dp.location_city,
    SUM(count) AS most_comom_arrivals_pleasure
FROM fact_imigration fi
LEFT JOIN dim_imigrant di ON di.imigrant_id = fi.imigrant_id
LEFT JOIN dim_visa_motive dvm ON dvm.visa_motive_id = di.visa_motive_id
LEFT JOIN dim_port dp ON dp.port_modal_id = fi.port_modal_id
WHERE fi.id_modal = 1
    AND dvm.motive = 'Pleasure'
GROUP BY 1
ORDER BY 2 DESC LIMIT 5
""")