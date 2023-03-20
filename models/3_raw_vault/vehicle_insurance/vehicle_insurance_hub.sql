
/* set yaml_metadata and endset are used for assigning variable ( key value pairs) */


{%- set yaml_metadata -%}
source_model: vehicle_insurance_stage_vw
src_pk: VEHICLE_INSURANCE_HK 
src_nk: 
    - VEHICLE_INSURANCE_ID
    
src_ldts: ETL_LOAD_DATETIME
src_source: RECORD_SOURCE
{%- endset -%}

/* fromyml() built-in jinja function  assigns values to variables */

{% set metadata_dict = fromyaml(yaml_metadata) %}



/*
In general, Hubs consist of 4 columns, described below.

Primary Key (src_pk)
A primary key (or surrogate key) which is usually a hashed representation of the natural key.

Natural Key / Business Key (src_nk)
This is usually a formal identification for the record, such as a incident ID. Usually called the business key because this value has meaning in business processes such as transactions and events.

Load date (src_ldts)
A load date or load date timestamp. This identifies when the record was first loaded into the database.

Record Source (src_source)
The source for the record.

Refer to following link for additional info https://dbtvault.readthedocs.io/en/latest/tutorial/tut_hubs/
*/

{{ dbtvault.hub(src_pk=metadata_dict["src_pk"],
                src_nk=metadata_dict["src_nk"], 
                src_ldts=metadata_dict["src_ldts"],
                src_source=metadata_dict["src_source"],
                source_model=metadata_dict["source_model"]) }}