/* set yaml_metadata and endset are used for assigning variable ( key value pairs) *//* fromyml() built-in jinja function  assigns values to variables */




/*
dbtvault will generate a stage using parameters provided in the next steps.
The recommended materialization for a stage is view, as the stage layer contains minimal transformations on the raw staging layer which need to remain in sync. 
The "source model" for a stage is coming from src_<tablename>_vw
the "derived_columns" derives source, effective_from & end_date
the "hashed_columns" represent columns to be hashed on business keys 
the "ranked_columns" and "null_columns" are not used 
Refer to following link for additional info https://dbtvault.readthedocs.io/en/latest/tutorial/tut_staging/
*/


-- Generated by dbtvault.

    

WITH source_data AS (

    SELECT

    vehicle_insurance_id,
    gender,
    age,
    driving_license,
    region_code,
    previously_insured,
    vehicle_age,
    vehicle_damage,
    annual_premium,
    policy_sales_channel,
    vintage,
    rec_create_date,
    rec_update_date,
    rec_create_by,
    rec_update_by

    FROM "mydw"."mydw_stage_vault"."src_vehicle_insurance_vw"
),

derived_columns AS (

    SELECT

    vehicle_insurance_id,
    gender,
    age,
    driving_license,
    region_code,
    previously_insured,
    vehicle_age,
    vehicle_damage,
    annual_premium,
    policy_sales_channel,
    vintage,
    rec_create_date,
    rec_update_date,
    rec_create_by,
    rec_update_by,
    'eirssys' AS source,
    current_timestamp AS etl_load_datetime,
    current_date AS effective_from,
    to_date('9999-12-31','YYYY-MM-DD') AS end_date

    FROM source_data
),

hashed_columns AS (

    SELECT

    VEHICLE_INSURANCE_ID,
    GENDER,
    AGE,
    DRIVING_LICENSE,
    REGION_CODE,
    PREVIOUSLY_INSURED,
    VEHICLE_AGE,
    VEHICLE_DAMAGE,
    ANNUAL_PREMIUM,
    POLICY_SALES_CHANNEL,
    VINTAGE,
    REC_CREATE_DATE,
    REC_UPDATE_DATE,
    REC_CREATE_BY,
    REC_UPDATE_BY,
    SOURCE,
    ETL_LOAD_DATETIME,
    EFFECTIVE_FROM,
    END_DATE,

    CAST(UPPER(MD5(NULLIF(CONCAT_WS('||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(vehicle_insurance_id AS VARCHAR))), ''), '^^')
    ), '^^'))) AS BYTEA) AS vehicle_insurance_hk,
    CAST(UPPER(MD5(CONCAT_WS('||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(Age AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Annual_Premium AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Driving_License AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Gender AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Policy_Sales_Channel AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Previously_Insured AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Region_Code AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Vehicle_Age AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Vehicle_Damage AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(vehicle_insurance_id AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(Vintage AS VARCHAR))), ''), '^^')
    ))) AS BYTEA) AS hashdiff

    FROM derived_columns
),

columns_to_select AS (

    SELECT

    VEHICLE_INSURANCE_ID,
    GENDER,
    AGE,
    DRIVING_LICENSE,
    REGION_CODE,
    PREVIOUSLY_INSURED,
    VEHICLE_AGE,
    VEHICLE_DAMAGE,
    ANNUAL_PREMIUM,
    POLICY_SALES_CHANNEL,
    VINTAGE,
    REC_CREATE_DATE,
    REC_UPDATE_DATE,
    REC_CREATE_BY,
    REC_UPDATE_BY,
    SOURCE,
    ETL_LOAD_DATETIME,
    EFFECTIVE_FROM,
    END_DATE,
    VEHICLE_INSURANCE_HK,
    HASHDIFF

    FROM hashed_columns
)

SELECT * FROM columns_to_select