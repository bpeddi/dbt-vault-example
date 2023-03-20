
      insert into "mydw"."mydw_raw_vault"."vehicle_insurance_hub" ("vehicle_insurance_hk", "vehicle_insurance_id", "etl_load_datetime", "source")
    (
        select "vehicle_insurance_hk", "vehicle_insurance_id", "etl_load_datetime", "source"
        from "vehicle_insurance_hub__dbt_tmp154546154706"
    )


  