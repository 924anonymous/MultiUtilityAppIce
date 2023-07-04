import module.db_ingestion.insert_queries as queries
import streamlit as st
import GetData as do
import SnowflakeDbConnAndOperations as snowdb


# function for query generation

# """
# Purpose : it is creating single querie for respective db
# input : user input from UI
# output : it will insert data into db
# """

def singleQueryGenerator(option):
    doobj = do.ExecuteQueriesOnIceberg()
    snowdbobj = snowdb.ExecuteQueriesOnSnowflake(**st.secrets["snowflake"])
    if option.lower() == 'db_ingestion_config_details':
        with st.form('single_table_form'):

            query = 'select distinct database_type from db_conn_details;'
            data = doobj.execute_query(query=query, database='config')
            db_type = data['database_type'].tolist()

            database_type = st.selectbox('Select Database Type', db_type)

            col1, col2 = st.columns(2)
            col3, col4 = st.columns(2)

            with col1:
                db_name = st.text_input('Source Database Name', key=1)
                table_name = st.text_input('Source Table Name', key=3)
                columns_to_select_check = st.selectbox('Columns To Select Check', ('No', 'Yes'))
                limitDataCheck = st.selectbox('Data Check Limit', ('No', 'Yes'))
                incremental_cond_check = st.selectbox('Incremental Condition Check', ('No', 'Yes'))

                st.divider()

            with col2:
                schema_name = st.text_input('Source Schema Name', key=2)
                st.text_input('')
                columns_to_select = st.text_input('Columns To Select', key=4)
                limitDataConstraint = st.text_input('Set Limit Data Constraints', key=13)
                incr_column_name = st.text_input('Incremental Column Name', key=12)

                st.divider()

            with col3:
                mode = st.selectbox('Write Mode', ('Select Mode..', 'Overwrite', 'Append'))
                if mode.lower() == 'select mode..':
                    mode = ''
                src_bucket_name = st.text_input('S3 Source Bucket Name', key=21)
                raw_bucket_name = st.text_input('S3 Raw Bucket Name', key=6)
                curated_bucket_name = st.text_input('S3 Curated Bucket Name', key=8)

            with col4:
                st.text_input('', key=20)
                src_path = st.text_input('S3 Source Folder Path', key=5)
                raw_tgt_path = st.text_input('S3 Raw Folder Path', key=7)
                curated_tgt_path = st.text_input('S3 Curated Folder Path', key=9)

                incr_column_timestamp_cond_check = 'no'  # st.selectbox('Incremental Column Timestamp Condition Check',('No','Yes'))
                if incr_column_timestamp_cond_check.lower() == 'yes':
                    incr_column_timestamp_format = st.text_input('Incremental Column Timestamp Format', key=16)
                else:
                    incr_column_timestamp_format = ''

            query = f"select distinct id from db_conn_details where database_name ='{db_name}' and database_type = '{database_type}';"
            connection_id = None
            if db_name != '' and db_name is not None:
                data = doobj.execute_query(query=query, database='config')
                connection_id = int(data['id'])

            update_query_db_ing = queries.update_dbingestion_config_details.format(database_name=db_name,
                                                                                   schema_name=schema_name,
                                                                                   table_name=table_name)

            query = queries.db_ingestion_config_details_query.format(db_name=db_name, schema_name=schema_name,
                                                                     table_name=table_name,
                                                                     columns_to_select=columns_to_select,
                                                                     src_path=src_path,
                                                                     raw_bucket_name=raw_bucket_name,
                                                                     raw_tgt_path=raw_tgt_path,
                                                                     curated_bucket_name=curated_bucket_name,
                                                                     curated_tgt_path=curated_tgt_path,
                                                                     mode=mode.lower(),
                                                                     limitDataCheck=limitDataCheck.lower(),
                                                                     incr_column_name=incr_column_name,
                                                                     limitDataConstraint=limitDataConstraint,
                                                                     incremental_cond_check=incremental_cond_check.lower(),
                                                                     columns_to_select_check=columns_to_select_check.lower(),
                                                                     incr_column_timestamp_format=incr_column_timestamp_format,
                                                                     incr_column_timestamp_cond_check=incr_column_timestamp_cond_check.lower(),
                                                                     connection_id=str(connection_id))
            submitted = st.form_submit_button("Insert Data Into DB", type='primary')
            if submitted:
                doobj.execute_dml_query(query=update_query_db_ing, database='config')
                print(query)
                doobj.execute_dml_query(query=query, database='config')
                st.success('Data Inserted Into DB Successfully')

    elif option.lower() == "incremental_config_details":
        with st.form("inc_dtl"):
            left_col, right_col = st.columns(2)
            with left_col:
                db_name = st.text_input('Source Database Name')
                table_name = st.text_input('Source Table Name')
                column_name = st.text_input('Column Name')
            with right_col:
                schema_name = st.text_input('Source Schema Name')
                operator = st.text_input('Operator ("<" or ">" or "=")')
                value = st.text_input('Column Value')

            update_query_inc_dtl = queries.update_incremental_details.format(database_name=db_name,
                                                                             schema_name=schema_name,
                                                                             table_name=table_name)

            inc_query = queries.incremental_config_details_query.format(db_name=db_name,
                                                                        schema_name=schema_name,
                                                                        table_name=table_name,
                                                                        value=value,
                                                                        column_name=column_name,
                                                                        operator=operator)

            submitted = st.form_submit_button("Insert Data Into DB", type='primary')
            if submitted:
                doobj.execute_dml_query(query=update_query_inc_dtl, database='config')
                doobj.execute_dml_query(query=inc_query, database='config')
                st.success('Data Inserted Into DB Successfully')

    elif option.lower() == "dbingestion_block_driver_details":
        with st.form("block_dtl"):
            left_col, right_col = st.columns(2)
            with left_col:
                db_name = st.text_input('Source Database Name')

                query = 'select distinct table_name from dbingestion_config_details;'
                data = doobj.execute_query(query=query, database='config')
                table_name_list = data['table_name'].tolist()

                table_name = st.multiselect('Source Table Name', table_name_list)
                st.write(table_name)

            with right_col:
                schema_name = st.text_input('Source Schema Name')
                block_number = st.text_input('Block Number')

            block_query = queries.dbingestion_block_driver_details_query.format(db_name=db_name,
                                                                                schema_name=schema_name,
                                                                                table_name=",".join(table_name),
                                                                                block_number=block_number)

            submitted = st.form_submit_button("Insert Data Into DB", type='primary')
            if submitted:
                doobj.execute_dml_query(query=block_query, database='config')
                st.success('Data Inserted Into DB Successfully')

    elif option.lower() == "sf_load_config_details":
        with st.form('sf_load_config_dtls'):
            left_col, right_col = st.columns(2)
            with left_col:
                db_name = st.text_input('Database Name')
                schema_name = st.text_input('Schema Name')
                table_name = st.text_input('Table Name')
                raw_table_name = st.text_input('Raw Table Name')
                force = st.selectbox('Force Load', ('FALSE', 'TRUE'))

            with right_col:
                file_prefix_path = st.text_input('File Prefix Path')
                sf_ext_stage = st.text_input('SF External Stage')
                load_type = st.selectbox('Load Type', ('as_is', 'custom'))
                truncate_load = st.selectbox('Truncate Load', ('yes', 'no'))

            load_config_query = queries.SF_LOAD_CONFIG_DETAILS.format(
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                raw_table_name=raw_table_name,
                file_prefix_path=file_prefix_path,
                sf_ext_stage=sf_ext_stage,
                load_type=load_type,
                truncate_load=truncate_load,
                force=force
            )
            submitted = st.form_submit_button("Insert Data Into DB", type='primary')
            if submitted:
                try:
                    snowdbobj.execute_dml_query(load_config_query)
                    st.success('Data Inserted Into DB Successfully')
                except Exception as e:
                    st.error(e)

    elif option.lower() == "sf_dq_config_details":
        with st.form('sf_dq_config_dtls'):
            left_col, right_col = st.columns(2)
            with left_col:
                db_name = st.text_input('Database Name')
                schema_name = st.text_input('Schema Name')
                table_name = st.text_input('Table Name')
                source_database_name = st.text_input('Source Database Name')
                source_schema_name = st.text_input('Source Schema Name')
            with right_col:
                source_table_name = st.text_input('Source Table Name')
                target_database_name = st.text_input('Target Database Name')
                target_schema_name = st.text_input('Target Schema Name')
                target_table_name = st.text_input('Target Table Name')
                skip_dq = st.selectbox('Skip DQ', ('yes', 'no'))

            dq_config_query = queries.SF_DQ_CONFIG_DETAILS.format(
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                source_database_name=source_database_name,
                source_schema_name=source_schema_name,
                source_table_name=source_table_name,
                target_database_name=target_database_name,
                target_schema_name=target_schema_name,
                target_table_name=target_table_name,
                skip_dq=skip_dq
            )
            submitted = st.form_submit_button("Insert Data Into DB", type='primary')
            if submitted:
                try:
                    snowdbobj.execute_dml_query(dq_config_query)
                    st.success('Data Inserted Into DB Successfully')
                except Exception as e:
                    st.error(e)
