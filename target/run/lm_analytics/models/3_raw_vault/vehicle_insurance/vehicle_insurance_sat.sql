
      insert into "mydw"."mydw_raw_vault"."vehicle_insurance_sat" ("vehicle_insurance_hk", "hashdiff", "vehicle_insurance_id", "gender", "age", "driving_license", "region_code", "previously_insured", "vehicle_age", "vehicle_damage", "annual_premium", "policy_sales_channel", "vintage", "rec_create_date", "rec_update_date", "rec_create_by", "rec_update_by", "effective_from", "etl_load_datetime", "source")
    (
        select "vehicle_insurance_hk", "hashdiff", "vehicle_insurance_id", "gender", "age", "driving_license", "region_code", "previously_insured", "vehicle_age", "vehicle_damage", "annual_premium", "policy_sales_channel", "vintage", "rec_create_date", "rec_update_date", "rec_create_by", "rec_update_by", "effective_from", "etl_load_datetime", "source"
        from "vehicle_insurance_sat__dbt_tmp154546675226"
    )


  