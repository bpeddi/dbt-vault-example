with 

--- Select data from vehicle_insurance satellite 
cte_vehicle_insurance_satellite as (
select
    VEHICLE_INSURANCE_HK,
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
from 
  {{ref('vehicle_insurance_sat')}}  stg

{% if is_incremental() %}

left outer join (
select 
    distinct a.VEHICLE_INSURANCE_ID as TGT_VEHICLE_INSURANCE_ID 
from {{ this }} a ) as tgt
    on stg.VEHICLE_INSURANCE_ID=tgt.TGT_VEHICLE_INSURANCE_ID
where (tgt.TGT_VEHICLE_INSURANCE_ID is null )

{% endif %}

) ,

--- Prepare final table 

final as (
select 
    row_number() over (order by VEHICLE_INSURANCE_HK) as vehicle_insurance_dim_id ,
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
from cte_vehicle_insurance_satellite
)

-- Final output is selected here 

select * from final

