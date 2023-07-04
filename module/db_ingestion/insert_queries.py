import datetime
import json
# """
# Purpose : input parameters for queryGenerator function
# """

db_ingestion_config_details_query = '''
insert into dbingestion_config_details (
 DATABASE_NAME,
 SCHEMA_NAME,
 TABLE_NAME,
 CONFIGURATIONS,
 CREATED_AT,
 STATUS,
 CREATED_USER,
 CONNECTION_ID) VALUES('{db_name}',
'{schema_name}',
'{table_name}',
'{{\"database_name\": \"{db_name}\", 
\"schema_name\": \"{schema_name}\",
\"table_name\": \"{table_name}\",
\"columns_to_select\": \"{columns_to_select}\", 
\"src_path\": \"{src_path}\", 
\"raw_bucket_name\": \"{raw_bucket_name}\",
\"raw_tgt_path\": \"{raw_tgt_path}\",
\"curated_bucket_name\": \"{curated_bucket_name}\",  
\"curated_tgt_path\": \"{curated_tgt_path}\", 
\"mode\": \"{mode}\", 
\"header": \"\", 
\"delimiter\": \"\", 
\"hashcheck\": \"no\", 
\"fileFormat\": \"parquet\", 
\"part_files\": \"NA\", 
\"comp_format\": \"\", 
\"target_type\": \"S3\", 
\"col_name_lb_ub\": \"\", 
\"hashcolumnname\": \"\", 
\"limitDataCheck\": \"{limitDataCheck}\",  
\"rds_config_mode\": \"append\", 
\"compressionCheck\": \"no\",
\"eventDtPartition\": {{\"ipColName\": \"\", \"ipDateFormat\": \"\", \"opDateFormat": \"\", \"partitionFlag\": \"no\", \"partColumnName\": \"\"}},
\"incr_column_name\": \"{incr_column_name}\", 
\"query_contraints\": \"\", 
\"curated_prt_check\": \"no\", 
\"processDtPartition\": {{\"timeZone\": \"Asia/Kolkata\", \"dateFormat\": \"yyyyMMdd\", \"partitionFlag\": \"yes\",\"partColumnName\": \"aws_processed_date\"}}, 
\"processed_tgt_path\": \"\", 
\"additionalPartition\": \"\", 
\"limitDataConstraint\": \" {limitDataConstraint} \", 
\"writePartitionCheck\": \"yes\", 
\"loadDateDirPartition\": {{\"partitionFlag\": \"yes\"}}, 
\"processed_bucket_name\": \"\", 
\"incremental_cond_check\": \"{incremental_cond_check}\", 
\"processed_bucket_check\": \"no\", 
\"query_contraints_check\": \"no\", 
\"columns_to_select_check\": \"{columns_to_select_check}\",
\"target_file_prefix_path\": \"\", 
\"incr_column_timestamp_format\": \"{incr_column_timestamp_format}\",
\"incr_column_timestamp_cond_check\": \"{incr_column_timestamp_cond_check}\"}}',
'{CURRENT_TIMESTAMP}',
'active',
'streamlit_app',
{connection_id});'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

sfload_config_details_query = '''
insert into sfload_config_details(
 DATABASE_NAME,
 SCHEMA_NAME,
 TABLE_NAME,
 CONFIG_DTLS,
 STATUS,
 CREATED_AT,
 CREATED_USER) VALUES('{db_name}',
'{schema_name}',
'{table_name}',
'{{\"database_name\": \"{db_name}\",
\"schema_name\": \"{schema_name}\",
\"table_name\": \"{table_name}\",
\"file_prefix_path\": \"{s3_path}\",
\"sf_ext_stage\": \"{ext_stage}\",
\"load_type\": \"{load_type}\",
\"truncate_load\": \"{truncate_load}\",
\"purge_files\": \"{purge_file}\",
\"action_on_error\":\"{action_on_error}\",
\"column_names_caps\": \"no\",
\"file_prefix_name_case\": \"as_is\",
\"_etl_batch_id\": \"yes\",
\"_etl_insert_date_time\": \"yes\",
\"_etl_update_date_time\": \"yes\",
\"force\":\"FALSE\",
\"partition_cols\": [],
\"validate_errors\":\"no\",
\"error_table_name\" : \"\",
\"error_schema_name\" : \"\",
\"offload\":\"no\",
\"stage_name\": \"\",
\"folder_name\" : \"\",
\"query_columns\" : \"\",
\"condition\" : \"\",
\"limit_value\" : \"\"}}',
'active',
'{CURRENT_TIMESTAMP}',
'streamlit_app');'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

sf_load_stg_config_query = '''
insert into sf_load_stg_config (
CONFIGURATIONS,
BLOCK_NUMBER,
STATUS,
CREATED_AT,
CREATED_BY,
TARGET_SCHEMA,
TARGET_TABLE,
TARGET_DATABASE) VALUES ('{{\"source_table_schema\": \"{source_table_schema}\",
\"target_table_schema\": \"{target_table_schema}\",
\"source_table_name\": \"{source_table_name}\",
\"target_table_name\": \"{target_table_name}\",
\"source_table_database\": \"{source_table_database}\",
\"target_table_database\": \"{target_table_database}\"}}',
'{block_number}',
'active',
'{CURRENT_TIMESTAMP}',
'streamlit_app',
'{target_schema}',
'{target_table}',
'{target_database}');'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

sf_load_curated_config_query = '''
insert into sf_load_curated_config(
 CONFIGURATIONS,
 BLOCK_NUMBER,
 STATUS,
 CREATED_AT,
 CREATED_BY,
 TARGET_SCHEMA,
 TARGET_TABLE,
 TARGET_DATABASE) VALUES('{{\"source_table_schema\": \"{source_table_schema}\",
\"target_table_schema\": \"{target_table_schema}\",
\"source_table_database\": \"{source_table_database}\",
\"target_table_database\": \"{target_table_database}\",
\"source_table_name\": \"{source_table_name}\",
\"target_table_name\": \"{target_table_name}\",
\"dml_type\": \"{dml_type}\"}}',
'{block_number}',
'active',
'{CURRENT_TIMESTAMP}',
'streamlit_app',
'{target_schema}',
'{target_table}',
'{target_database}');'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

incremental_config_details_query = '''
insert into incremental_details(
 DATABASE_NAME,
 SCHEMA_NAME,
 TABLE_NAME,
 VALUE,
 COLUMN_NAME,
 OPERATOR,
 STATUS,
 CREATED_AT,
 CREATED_USER) VALUES('{db_name}',
 '{schema_name}',
 '{table_name}', 
 '{value}', 
 '{column_name}',
 '{operator}',
'active',
'{CURRENT_TIMESTAMP}',
'streamlit_app');'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

sf_load_block_driver_details = '''
INSERT INTO SF_LOAD_BLOCK_DRIVER_DETAILS (SNOWFLAKE_DATABASE_MASTER,
SNOWFLAKE_SCHEMA_MASTER,
TABLE_MASTER,
 BLOCK_NUMBER,
 CREATE_DATE,
 CREATED_BY) VALUES ('{sf_db_master}',
 '{sf_schema_master}',
 '{table_master}',
 '{block_number}',
 '{CURRENT_TIMESTAMP}',
 'streamlit_app');'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

dbingestion_block_driver_details_query = '''
INSERT INTO DBINGESTION_BLOCK_DRIVER(SOURCE_DATABASE_MASTER,
 SCHEMA_MASTER,
 TABLE_MASTER,
 BLOCK_NUMBER,
 CREATED_AT,
 CREATED_USER) VALUES('{db_name}',
 '{schema_name}',
 '{table_name}',
 '{block_number}',
 '{CURRENT_TIMESTAMP}',
 'streamlit_app');'''.replace("{CURRENT_TIMESTAMP}", str(datetime.datetime.now()))

incremental_table_list_query = '''
INSERT INTO dev_code_repo.COM.INCREMENTAL_TABLES_LIST(
DATABASE_NAME,
SCHEMA_NAME,
TABLE_NAME,
STATUS) VALUES(
'{database_name}',
 '{schema_name}',
 '{table_name}',
 '{status}'); '''

update_incremental_details = '''update incremental_details 
set status='inactive'
where database_name = '{database_name}'
and schema_name = '{schema_name}'
and table_name = '{table_name}' and status = 'active';'''

update_dbingestion_config_details = '''
update dbingestion_config_details
set status = 'inactive'
where database_name = '{database_name}'
and schema_name = '{schema_name}'
and table_name = '{table_name}' and status = 'active';'''


SF_LOAD_CONFIG_DETAILS = '''insert into CODE_REPO.DATA_ACCELERATOR_SCHEMA.SF_LOAD_CONFIG_DETAILS_PROD
(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CONFIG_DTLS, STATUS, CREATE_DATE, CREATED_BY)
VALUES(
'{db_name}',
'{schema_name}',
'{table_name}',
'{{\"database_name\": \"{db_name}\",
\"schema_name\": \"{schema_name}\",
\"table_name\": \"{raw_table_name}\",
\"file_prefix_path\": \"{file_prefix_path}\",
\"sf_ext_stage\": \"COMMON_OBJECTS.{sf_ext_stage}\",
\"executed_sp\": \"DATA_ACCELERATOR_SCHEMA.GENERIC_LOAD_STG_TABLE_SP\",
\"load_type\": \"{load_type}\",
\"batch_id\": \"no\",
\"insert_date_time\": \"no\",
\"update_date_time\": \"no\",
\"truncate_load\": \"{truncate_load}\",
\"purge_files\": \"no\",
\"force\":\"{force}\",
\"action_on_error\":\"continue\",
\"validate_errors\":\"no\",
\"offload\":\"no\",
\"error_table_name\" : \"SAVE_COPY_ERROR\",
\"error_schema_name\" : \"DATA_ACCELERATOR_SCHEMA\",
\"stage_name\": \"DATA_ACCELERATOR_SCHEMA.EXT_STAGE_UNLOAD_CSV\",
\"folder_name\" : \"snowflake-blazeclan/unload\",
\"query_columns\" : \"\",
\"condition\" : \"\",
\"limit_value\" : \"10\"
}}',
'active',
CURRENT_TIMESTAMP(),
'Streamlit_App');'''

SF_DQ_CONFIG_DETAILS = '''insert into CODE_REPO.DATA_ACCELERATOR_SCHEMA.SF_DQ_CONFIG_DETAILS_PROD
(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CONFIG_DTLS, STATUS, CREATE_DATE, CREATED_BY)
VALUES(
'{db_name}',
'{schema_name}',
'{table_name}',
'{{\"source_database_name\": \"{source_database_name}\",
\"source_schema_name\": \"{source_schema_name}\",
\"source_table_name\": \"{source_table_name}\",
\"target_database_name\":\"{target_database_name}\",
\"target_schema_name\":\"{target_schema_name}\",
\"target_table_name\":\"{target_table_name}\",
\"skip_dq\":\"{skip_dq}\",
\"error_schema_name\":\"DATA_ACCELERATOR_SCHEMA\",
\"error_table_name\":\"DQ_ERROR_TABLE\",
\"error_offload\":\"no\",
\"executed_sp\": \"DATA_ACCELERATOR_SCHEMA.GENERIC_FETCH_ERROR_RECORDS_SP\",
\"error_offload_config\":{{
\"stage_name\": \"DATA_ACCELERATOR_SCHEMA.EXT_STAGE_RDB_ORC\",
\"folder_name\": \"globe/offload/new_device\",
\"query_columns\": \"\",
\"condition\": \"yes\",
\"limit_value\": \"\"
}},
\"upload_to_target\":\"yes\",
\"upload_to_target_config\":{{\"executed_sp\": \"DATA_ACCELERATOR_SCHEMA.GENERIC_FETCH_CLEAN_RECORDS_SP\"}}
}}',
'active',
CURRENT_TIMESTAMP(),
'Streamlit_App');'''
