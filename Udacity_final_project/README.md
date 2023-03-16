14/03: Keep working on the `sql_queries.py` file. I will have to create a configuration file also.

--------------------------------------------
# Udacity's Capstone Project

## Tables needed
`airport-codes_csv.csv`
`imigration_data_sample.csv`
`I94_SAS_Labels_Descriptions.SAS`

Since all the data in `I94_SAS_Labels_Descriptions.SAS` are formatted in a particular way we need to run the `reading_sas_file.py` first to retrieve all the data we need. After running it five files will be generated: `i94addr.csv`, `i94cit_res.csv`, `i94mode.csv`, `i94port.csv` and `i94visa.csv`. Those files, with the `airport-codes_csv.csv` and `imigration_data_sample.csv`, will help us generate all the staging tables needed to create the tables for our final schema.

## Staging tables exploration for necessary data wrangling in the future

### `staging_airport_codes`
Table generated from `airport-code_csv.csv` table. 
This table presented itself with a little challenge because redshift was reading the comma inside the double quote as a delimiter. So, it understood that this table had one more column than it actually did. To fix this problem I used the strategy used in this [StackOverflow post](https://stackoverflow.com/questions/47290137/redshift-loading-csv-with-commas-in-a-text-field). This strategy was used in all 3 staging tables.

### `staging_imigration`
Table generated from the imigrant information available in `immigration_data_sample.csv`/parquet files.

### `sas_descriptive_information`
Table generated from the `I94_SAS_Labels_Descriptions.SAS` using the `reading_sas_file.py`. The script in .py file generates a series of tables containing the columns: 'id', 'description' and 'column'. The column 'column' refers to the column from the imigrant table which we are generating the information.
SCHEMA:
id: STRING
description: STRING
column: STRING

## Data Schema To Be Generated

FACT TABLE: 
    SOURCE: immigration_data_sample.csv / parquet files
    COLUMNS:
    fact_imigration
        id [PRIMARY KEY]                                {AUTOMATIC GENERATION}
        imigrant_id [cicid]                             {IMIGRANT TABLE}
        id_location_residence [i94res - CODE]           {IMIGRANT TABLE}
        id_port_arrival_us [i94port - CODE]             {IMIGRANT TABLE}
        id_location_residence_option2 [i94addr - CODE]  {IMIGRANT TABLE}
        count - No need to mess with it                 {IMIGRANT TABLE}
        flight_number [fltno]                           {IMIGRANT TABLE}


DIMENSIONS:

    dim_imigrant:
        imigrant_id [cicid] (KEY)                       {IMIGRANT TABLE}
        gender                                          {IMIGRANT TABLE}
        occupation [occup]                              {IMIGRANT TABLE}
        birth_year [biryear]                            {IMIGRANT TABLE}
        admission_number [admnum]                       {IMIGRANT TABLE}
        visa_id                                         {AUTOMATICALLY GENERATED ON dim_visa}
        birth_place (i94cit)                            {IMIGRANT TABLE}
        residence (i94res)                              {IMIGRANT TABLE}

    dim_visa:
        visa_id                                         {CREATE}      
        visa_motive_id                                  {sas_descriptive_information.csv - filter column = 'i94visa'}
        visa_issued_place (VISAPOST)                    {IMIGRANT TABLE}
        visa_type (VISATYPE)                            {IMIGRANT TABLE}

    OK  dim_visa_motive
            visa_motive_id                              {sas_descriptive_information.csv - filter column = 'i94visa'}
            motive                                      {sas_descriptive_information.csv - filter column = 'i94visa'}
# ----------------------
# WORKING ON DIM_PORT   
# ----------------------  
    dim_port:
        i94port (KEY)                                   {IMIGRANT TABLE}
        id_modal [i94mode - CODE]                       {dim_modal}
        type                                            {`airport-code_csv`}
        name                                            {`airport-code_csv`}
        continent                                       {`airport-code_csv`}
        iso_country                                     {`airport-code_csv`}

    OK  dim_modal
            id_modal                                 {IMIGRANT TABLE}
            modal                                    {sas_descriptive_information.csv - filter column = 'i94mode'}

OK  dim_us_states
        state_id                                        {sas_descriptive_information.csv - filter column = 'i94addr'}
        state                                           {sas_descriptive_information.csv - filter column = 'i94addr'}
            Remember that that are some cases that are not states. Create a new id for those.

OK  dim_country
        country_id                                      {sas_descriptive_information.csv - filter column = i94cit_res'}                                                                        
        country                                         {sas_descriptive_information.csv - filter column = 'i94cit_res'}

## Possibilities
In case I decide to improve the code adding the airline company name we can add the column 'airline_name' in the `dim_flight` table, we need to perform some webscrapping:
Working at the webscrapping code for airline information. 
may help: https://medium.com/geekculture/web-scraping-tables-in-python-using-beautiful-soup-8bbc31c5803e
Data necessary to bring the airline company name and country
Can scrape here: https://zbordirect.com/en/tools/iata-airlines-codes

|-------- NOT GOING TO BE USED --------|
|But in case we come back to this table|
|we can bring city_id to dim_port and  |
|use it.                               |
    dim_city
        city_id
        city
        state
        total_population
        foreign_born (Foreign-born)

    dim_flight:
        fltno                                           {IMIGRANT TABLE}
        airline                                         {IMIGRANT TABLE}