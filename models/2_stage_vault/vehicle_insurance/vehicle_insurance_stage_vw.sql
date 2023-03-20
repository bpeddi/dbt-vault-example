
/* set yaml_metadata and endset are used for assigning variable ( key value pairs) */


{%- set yaml_metadata -%}
source_model: src_vehicle_insurance_vw
  
derived_columns:
  source: "!eirssys"
  etl_load_datetime: "current_timestamp"
  effective_from: "current_date"
  end_date: "to_date('9999-12-31','YYYY-MM-DD')"
hashed_columns:
  vehicle_insurance_hk:
    - "vehicle_insurance_id"
  hashdiff:
    is_hashdiff: true
    columns:
      - "vehicle_insurance_id"
      - "Gender"
      - "Age"
      - "Driving_License"
      - "Region_Code"
      - "Previously_Insured"
      - "Vehicle_Age"
      - "Vehicle_Damage"
      - "Annual_Premium"
      - "Policy_Sales_Channel"
      - "Vintage"
{%- endset -%}

/* fromyml() built-in jinja function  assigns values to variables */

{% set metadata_dict = fromyaml(yaml_metadata) %}


/*
dbtvault will generate a stage using parameters provided in the next steps.
The recommended materialization for a stage is view, as the stage layer contains minimal transformations on the raw staging layer which need to remain in sync. 
The "source model" for a stage is coming from src_<tablename>_vw
the "derived_columns" derives source, effective_from & end_date
the "hashed_columns" represent columns to be hashed on business keys 
the "ranked_columns" and "null_columns" are not used 
Refer to following link for additional info https://dbtvault.readthedocs.io/en/latest/tutorial/tut_staging/
*/


{{ dbtvault.stage(include_source_columns=true,
                  source_model=metadata_dict['source_model'],
                  derived_columns=metadata_dict['derived_columns'],
                  null_columns=none,
                  hashed_columns=metadata_dict['hashed_columns'],
                  ranked_columns=none) }}