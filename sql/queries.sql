create or replace storage integration stg_int_s3_aact
enabled  = true
storage_provider = 'S3'
storage_aws_role_arn = 'arn:aws:iam::669640508322:role/s3snowflakerole'
type  = external_stage
storage_allowed_locations = ('*')
comment = 'This is integration between AWS S3 and Snowflake for ingestion';

desc integration stg_int_s3_aact;

create stage ct_protocol.linkml.kipi_acct_stg
storage_integration = stg_int_s3_aact
url = 's3://kipi-snowflake-ctas-data/aact/';


list @ct_protocol.linkml.kipi_acct_stg;
desc warehouse compute_wh;
show warehouses;

create user dev_ingestion
type = service;

desc user dev_ingestion;
show users;

alter user dev_ingestion set rsa_public_key ='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxewsDLZuzni2n6OsjY5N
K3ZcWi+lnqjYrnandiCl+uY8fGF+USlLF377eXuB0Of+puLhRTGTk5FFDibqiPr0
J56Z++uUk3+aHNxFimGhzWzDG2Kaj3bz2e9Mw61WwwRaNh5G0U4B0N23mMpWVEog
9VjfmbuEG3+azW9yvSKZcDpmxtZVBFRfrSC1bsHY5PfLGd9nCxDz+wl7EfXL4aLu
RfnVrvZzxvboS+PPWNVT8TdjljVn/hl9Hr21AQWgc3XAZhi9t7o9mNAHr1QpUU6Y
gEALB2K8UO/12tpjxN8me0ngYCQeFV+cWH6Ej7NPN9B3HRgDiK4qxG7EZFm0ObhH
tQIDAQAB';

revoke role sysadmin from user dev_ingestion;
create role role_fr_dev_ingest;
grant role role_fr_dev_ingest to role sysadmin;
grant role role_fr_dev_ingest to user dev_ingestion;
grant usage on warehouse compute_wh to role role_fr_dev_ingest;
grant usage on database ct_protocol to role role_fr_dev_ingest;
grant usage on schema ct_protocol.linkml to role role_fr_dev_ingest;
grant create table on schema ct_protocol.linkml to role role_fr_dev_ingest;
grant select, update ,insert  on all tables in schema ct_protocol.linkml to role role_fr_dev_ingest;
grant usage on file format CT_PROTOCOL.LINKML.ff_aact_csv to role role_fr_dev_ingest;
grant usage on stage ct_protocol.linkml.kipi_acct_stg  to role role_fr_dev_ingest;

table CT_PROTOCOL.LINKML."AdverseEventReporting";

create file format CT_PROTOCOL.LINKML.ff_aact_csv
type = csv
skip_header = 1
trim_space = true
;