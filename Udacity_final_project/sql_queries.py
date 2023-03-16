
# DROP TABLES
staging_airport_codes_drop = """DROP TABLE IF EXISTS staging_airport_codes"""
staging_imigration_drop = """DROP TABLE IF EXISTS staging_imigration"""
staging_sas_drop = """DROP TABLE IF EXISTS staging_sas"""


# CREATE TABLES
staging_airport_codes_create_table = """
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
"""

staging_imigration_create_table = """
CREATE TABLE staging_imigration (
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
)
"""

staging_sas_create_table = """
CREATE TABLE staging_sas_information (
    id	VARCHAR,
    description	VARCHAR,
    column_name VARCHAR 
)
"""

dim_country_create = """
CREATE TABLE dim_country (
    id VARCHAR,
    country VARCHAR
)
"""

dim_state_create = """
CREATE TABLE dim_state (
    id VARCHAR,
    state VARCHAR
)
"""

dim_modal_create = """
CREATE TABLE dim_modality (
    id_modal VARCHAR,
    modal VARCHAR
)
"""

dim_visa_motive_create = """
CREATE TABLE dim_visa_motive (
    visa_motive_id VARCHAR,
    motive VARCHAR
)
"""

dim_port_create = """
CREATE TABLE dim_port (
    port_id VARCHAR,
    modal_id VARCHAR,
    port_type VARCHAR,
    port_name VARCHAR,
    port_country VARCHAR,
    port_city VARCHAR
)
"""

# STAGING TABLES

staging_airport_codes_copy = ("""
    copy staging_airport_codes FROM '{}'
    credentials 'aws_iam_role={}'
    region {}
    delimiter ',' EMPTYASNULL CSV NULL AS '\0'
    IGNOREHEADER 1;
""").format(AIRPORT_CODE_DATA, ARN, REGION)

staging_imigration_copy = ("""
    copy staging_imigration FROM '{}'
    credentials 'aws_iam_role={}'
    region {}
    delimiter ',' EMPTYASNULL CSV NULL AS '\0'
    IGNOREHEADER 1;
""").format(IMIGRATION_DATA, ARN, REGION)

staging_sas_information_copy = ("""
    copy staging_sas_information FROM '{}'
    credentials 'aws_iam_role={}'
    region {}
    delimiter '|' EMPTYASNULL CSV NULL AS '\0'
    IGNOREHEADER 1;
""").format(SAS_DATA, ARN, REGION)


# DIM TABLES

insert_into_dim_country = """
INSERT INTO dim_country (id, country)
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
"""

insert_into_dim_state = """
INSERT INTO dim_state (id, state)
SELECT
    id,
    description
FROM staging_sas_information
WHERE column_name = 'i94addr'
    AND id NOT IN ('99', 'GU', 'PR', 'VI')
ORDER BY 1 ASC
"""

insert_into_dim_modal = """
INSERT INTO dim_modal (id_modal, modal)
SELECT
    id AS id_modal,
    description AS modal
FROM staging_sas_information
WHERE column_name = 'i94mode'
ORDER BY 1 ASC
"""

inser_into_dim_visa_motive = """
INSERT INTO dim_visa_motive (visa_motive_id, motive)
SELECT
    id AS visa_motive_id,
    description AS motive
FROM staging_sas_information
WHERE column_name = 'i94visa'
ORDER BY 1 ASC
"""

# STILL WORKING HERE
insert_into_dim_port = """
SELECT DISTINCT
    si.i94port AS port_id,
    si.i94mode AS modal_id,
    CASE 
        WHEN dm.modal = 'Air' THEN sac.type ELSE 'UNKNOWN'
    END AS port_type,
    CASE 
        WHEN dm.modal = 'Air' THEN sac.name ELSE 'UNKNOWN'
    END AS port_name,
    CASE 
        WHEN dm.modal = 'Air' THEN sac.iso_country ELSE 'UNKNOWN'
    END AS port_country,
    CASE 
        WHEN dm.modal = 'Air' THEN sac.municipality ELSE 'UNKNOWN'
    END AS port_city
FROM staging_imigration si
LEFT JOIN staging_airport_codes sac ON si.i94port = sac.iata_code
LEFT JOIN dim_modal dm ON dm.id_modal = si.i94mode
WHERE port_type IS NOT NULL
"""