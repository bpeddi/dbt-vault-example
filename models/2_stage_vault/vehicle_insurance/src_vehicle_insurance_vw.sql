with 
-- Import CTEs
-- this query loads all data during full load and reads only delta during incremental load 
transform_vehicle_insurance as (
    select
        vehicle_insurance_id,
        gender,
        age,
        driving_license,
        case when region_code is null then 0 else region_code end as region_code,
        previously_insured,
        vehicle_age,
        vehicle_damage,
        annual_premium,
        policy_sales_channel,
        vintage,
        -- now() - interval '+1 day' as rec_create_date,
        -- now()::timestamp as rec_update_date,
        CURRENT_DATE() as rec_create_date,
        CURRENT_DATE() as rec_update_date,
        'dbt etl' as rec_create_by,
        'dbt etl' as rec_update_by

        from  {{ source('dbt_bpeddi_source','insurence_source') }}    

) ,

-- prepare the final table 

final as (

    select 
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
    from transform_vehicle_insurance 

)

-- Simple select statement
select * from final