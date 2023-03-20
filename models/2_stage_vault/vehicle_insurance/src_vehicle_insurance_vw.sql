with 
-- Import CTEs
-- this query loads all data during full load and reads only delta during incremental load 
transform_vehicle_insurance as (
    select
        VEHICLE_INSURANCE_ID,
        GENDER,
        AGE,
        DRIVING_LICENSE,
        CASE WHEN REGION_CODE IS NULL THEN 0 ELSE REGION_CODE END AS REGION_CODE,
        PREVIOUSLY_INSURED,
        VEHICLE_AGE,
        VEHICLE_DAMAGE,
        ANNUAL_PREMIUM,
        POLICY_SALES_CHANNEL,
        VINTAGE,
        -- NOW() - INTERVAL '+1 DAY' AS REC_CREATE_DATE,
        -- NOW()::TIMESTAMP AS REC_UPDATE_DATE,
        CURRENT_DATE() AS REC_CREATE_DATE,
        CURRENT_DATE() AS REC_UPDATE_DATE,
        'DBT ETL' AS REC_CREATE_BY,
        'DBT ETL' AS REC_UPDATE_BY

        from  {{ source('dbt_bpeddi_source','insurence_source') }}    

) ,

-- prepare the final table 

final as (

    select 
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
        REC_UPDATE_BY
    from transform_vehicle_insurance 

)

-- Simple select statement
select * from final