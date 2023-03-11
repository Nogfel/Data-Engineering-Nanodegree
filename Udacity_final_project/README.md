# Udacity's Capstone Project

## Tables needed
`airport-codes_csv.csv`
`imigration_data_sample.csv`
`I94_SAS_Labels_Descriptions.SAS`

Since all the data in `I94_SAS_Labels_Descriptions.SAS` are formatted in a particular way we need to run the `reading_sas_file.py` first to retrieve all the data we need. After running it five files will be generated: `i94addr.csv`, `i94cit_res.csv`, `i94mode.csv`, `i94port.csv` and `i94visa.csv`. Those files, with the `airport-codes_csv.csv` and `imigration_data_sample.csv`, will help us generate all the staging tables needed to create the tables for our final schema.

## Staging tables exploration for necessary data wrangling in the future

### `staging_airport_codes`


### `staging_imigration`


### `staging_sas_data`



## Data Schema To Be Generated

FACT TABLE: 
    SOURCE: immigration_data_sample.csv / parquet files
    COLUMNS:
        id [PRIMARY KEY]                                {AUTOMATIC GENERATION}
        imigrant_id [cicid]                             {ALREADY EXIXTS - JUST NEED TO RENAME}
        id_location_born [i94cit - key]                 {ALREADY EXIXTS - JUST NEED TO RENAME}
        id_location_residence [i94res - CODE]           {ALREADY EXIXTS - JUST NEED TO RENAME}
        id_port [i94port - CODE]                        {ALREADY EXIXTS - JUST NEED TO RENAME}
        id_location_residence_option2 [i94addr - CODE]  {ALREADY EXIXTS - JUST NEED TO RENAME}
        count - No need to mess with it                 {ALREADY EXIXTS}
        flight_number [fltno]                           {ALREADY EXIXTS - JUST NEED TO RENAME}


DIMENSIONS:

    dim_imigrant:
        imigrant_id [cicid] (KEY)                       {ALREADY EXIXTS - JUST NEED TO RENAME}
        gender                                          {ALREADY EXIXTS - JUST NEED TO RENAME}
        occupation [occup]                              {ALREADY EXIXTS - JUST NEED TO RENAME}
        birth_year [biryear]                            {ALREADY EXIXTS - JUST NEED TO RENAME}
        admission_number [admnum]                       {ALREADY EXIXTS - JUST NEED TO RENAME}
        visa_id                                         {AUTOMATICALLY GENERATED ON dim_visa}
        birth_place (i94cit)                            {ALREADY EXIXTS - JUST NEED TO RENAME}
        residence (i94res)

    dim_visa:
        visa_id                                         {AUTOMATICALLY GENERATED ON dim_visa}      
        visa_motive_id                                  {ALREADY EXISTS - COMES FROM `dim_visa_motive`} 
        visa_issued_place (VISAPOST)                    {ALREADY EXISTS}
        visa_type (VISATYPE)                            {ALREADY EXISTS}

        dim_visa_motive [(I94VISA -> 1 = Business | 2 = Pleasure | 3 = Student)]
            visa_motive_id                              {AUTOMATICALLY GENERATED ON dim_visa_motive}
            motive                                      {ALREADY EXISTS}
    
    dim_flight:
        fltno                                           {ALREADY EXISTS}
        airline                                         {ALREADY EXISTS}
        airline - Airline arrival in US (INT)           {DEPENDS ON WEBSCRAPING}
            

    dim_port:
        i94port (KEY)                                   {ALREADY EXISTS ON IMIGRANT TABLE}
        id_modality [i94mode - CODE]                    {ALREADY EXISTS ON IMIGRANT TABLE}
        type                                            {ALREADY EXISTS ON `airport-code_csv`}
        name                                            {ALREADY EXISTS ON `airport-code_csv`}
        continent                                       {ALREADY EXISTS ON `airport-code_csv`}
        iso_country                                     {ALREADY EXISTS ON `airport-code_csv`}

        dim_modality
            id_modality                                 {ALREADY EXISTS ON IMIGRANT TABLE}
            modality                                    


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


## Possibilities
In case I decide to perform some webscrapping:
Working at the webscrapping code for airline information. 
may help: https://medium.com/geekculture/web-scraping-tables-in-python-using-beautiful-soup-8bbc31c5803e
Data necessary to bring the airline company name and country
Can scrape here: https://zbordirect.com/en/tools/iata-airlines-codes