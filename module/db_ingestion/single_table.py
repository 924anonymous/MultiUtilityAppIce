import streamlit as st
from module.db_ingestion.singleQueryGenerator import singleQueryGenerator


def singleTable():
    # """
    # Purpose : for creating single insert query
    # input : taking user input
    # output : insert data into postgres sql database table
    # """

    option_dic = {
        'DB Ingestion Configuration Details': 'db_ingestion_config_details',
        'DB Incremental Configuration Details': 'incremental_config_details',
        'DB Ingestion Block Driver Details': 'dbingestion_block_driver_details',
        'SF Dq Config Details': 'sf_dq_config_details',
        'SF Load Config Details': 'sf_load_config_details'
    }

    single_option = st.selectbox(
        'Select Configuration Process Name : ',
        ('Select Process Type..', 'DB Ingestion Configuration Details', 'DB Incremental Configuration Details',
         'DB Ingestion Block Driver Details', 'SF Load Config Details', 'SF Dq Config Details'))
    if single_option.lower() == 'select process type..':
        st.write('Please Select Process Type..')
    else:
        st.write('Required Inputs..')
        try:
            query = singleQueryGenerator(option_dic[single_option])

        except Exception as e:
            st.text(f'Error : {e}')
