
/* set yaml_metadata and endset are used for assigning variable ( key value pairs) */


{%- set yaml_metadata -%}
source_model: src_vehicle_insurance_vw
  
derived_columns:
  RECORD_SOURCE: "!EIRSSYS"
  ETL_LOAD_DATETIME: "CURRENT_TIMESTAMP"
  EFFECTIVE_FROM: "CURRENT_DATE"
  END_DATE: "TO_DATE('9999-12-31','YYYY-MM-DD')"
hashed_columns:
  VEHICLE_INSURANCE_HK:
    - "VEHICLE_INSURANCE_ID"
  HASHDIFF:
    is_hashdiff: true
    columns:
      - "VEHICLE_INSURANCE_ID"
      - "GENDER"
      - "AGE"
      - "DRIVING_LICENSE"
      - "REGION_CODE"
      - "PREVIOUSLY_INSURED"
      - "VEHICLE_AGE"
      - "VEHICLE_DAMAGE"
      - "ANNUAL_PREMIUM"
      - "POLICY_SALES_CHANNEL"
      - "VINTAGE"
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