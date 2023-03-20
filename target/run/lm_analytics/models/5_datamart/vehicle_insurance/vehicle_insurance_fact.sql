
      
  
    

  create  table "mydw"."mydw_mart"."vehicle_insurance_fact"
  as (
    with 

--- Select data from vehicle_insurance satellite 
cte_vehicle_insurance_satellite as (
select
	vehicle_insurance_hk,
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
from 
  "mydw"."mydw_raw_vault"."vehicle_insurance_sat"  stg



) ,

--- Prepare final table 

final as (
select 
    row_number() over (order by vehicle_insurance_hk) as vehicle_insurance_dim_id ,
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
from cte_vehicle_insurance_satellite
)

-- Final output is selected here 

select * from final
  );
  
  