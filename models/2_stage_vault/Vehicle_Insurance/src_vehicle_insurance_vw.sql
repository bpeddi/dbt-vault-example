with 
-- Import CTEs
-- this query loads all data during full load and reads only delta during incremental load 
transform_vehicle_insurance as (
    select
        vehicle_insurance_id,
        Gender,
        Age,
        Driving_License,
        case when Region_Code is null then 0 else Region_Code end as Region_Code,
        Previously_Insured,
        Vehicle_Age,
        Vehicle_Damage,
        Annual_Premium,
        Policy_Sales_Channel,
        Vintage,
        NOW() - INTERVAL '+1 DAY' as rec_create_date,
        NOW()::timestamp as rec_update_date,
        'DBT ETL' as rec_create_by,
        'DBT ETL' as rec_update_by

        from  {{ source('dbt_test','vehicle_insurance') }}

) ,

-- prepare the final table 

final as (

    select 
        vehicle_insurance_id,
        Gender,
        Age,
        Driving_License,
        Region_Code,
        Previously_Insured,
        Vehicle_Age,
        Vehicle_Damage,
        Annual_Premium,
        Policy_Sales_Channel,
        Vintage,
        rec_create_date,
        rec_update_date,
        rec_create_by,
        rec_update_by
    from transform_vehicle_insurance 

)

-- Simple select statement
select * from final