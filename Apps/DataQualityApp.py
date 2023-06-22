import streamlit as st
import GetData
import Utility
import json
import os
import pandas as pd
from datetime import datetime


def dataquality_app():
    pd.set_option('display.max_colwidth', None)
    st.markdown("<h1 style='text-align: center;'>Data Quality</h1>", unsafe_allow_html=True)
    st.markdown("<style>footer {visibility: hidden;}</style>", unsafe_allow_html=True)
    try:
        doobj = GetData.ExecuteQueriesOnIceberg()
        log_df = doobj.execute_query(query='select * from error_table', database='erroneous')
    except Exception as e:
        st.error(e)
    else:
        try:
            if len(log_df) > 0:
                fdf = pd.DataFrame()

                fdf['created_at'] = log_df.apply(lambda x: json.loads(x['errorneous_records'])['created_at'], axis=1)
                fdf['updated_at'] = str(datetime.now())

                table_name = 'mining_data'

                log_df["errorneous_records"] = log_df.apply(
                    lambda row: Utility.key_operations(row["errorneous_records"], 'delete', '', '', table_name), axis=1)

                hide_table_row_index = """
                                        <style>
                                        thead tr th:first-child {display:none}
                                        tbody th {display:none}
                                        </style>
                                        """
                st.markdown(hide_table_row_index, unsafe_allow_html=True)

                all_rules_dict = {'All Rules': ['Null Check', 'Length Check', 'Boolean Check']}
                all_rules_df = pd.DataFrame(all_rules_dict)
                # list_of_all_tables = log_df['table_name'].unique()
                df_applied_rule = log_df.filter(items=['column_name', 'check_name']).drop_duplicates()
                df_applied_rule['fix_value'] = ''
                df_applied_rule['table_name'] = table_name

                with st.container():
                    left_col, right_col = st.columns(2)
                    with left_col:
                        st.table(all_rules_df)
                    with right_col:
                        # selected_table = st.selectbox(label='Select Table Name',
                        #                               options=list_of_all_tables)
                        # df_applied_rule_temp = log_df.filter(
                        #     items=['table_name', 'column_name', 'check_name']).drop_duplicates().query(
                        #     'table_name == @selected_table')
                        with st.form("form_fix"):
                            fi_col, le_col, mid_col, ri_col = st.columns(4)
                            for ind in df_applied_rule.index:
                                with fi_col:
                                    tbl_nm = st.text_input('Table Name', value=df_applied_rule['table_name'][ind])
                                with le_col:
                                    col_nm = st.text_input('Column Name', value=df_applied_rule['column_name'][ind])
                                with mid_col:
                                    chk_nm = st.text_input('Check Name', value=df_applied_rule['check_name'][ind])
                                with ri_col:
                                    f_val = st.text_input('Fix Value', value='', key=ind)
                            submitted = st.form_submit_button("Submit")
                            if submitted:
                                for _ in range(len(df_applied_rule)):
                                    log_df["errorneous_records"] = log_df.apply(
                                        lambda row: Utility.key_operations(row["errorneous_records"], 'add',
                                                                           col_nm.replace(table_name + '_', ''),
                                                                           f_val, ''),
                                        axis=1)

                                fdf['mining_data'] = log_df['errorneous_records']
                                file_name = "fixed_errorneous_records.parquet"
                                fdf.to_parquet(file_name, engine='auto', compression='snappy')
                        try:
                            if os.path.exists('./fixed_errorneous_records.parquet'):
                                with open('fixed_errorneous_records.parquet', "rb") as file:
                                    btn = st.download_button(
                                        label="Download Parquet File",
                                        data=file,
                                        file_name='fixed_erroneous_records_' + str(
                                            datetime.now().strftime("%d%m%Y%I%M%p")) + '.parquet'
                                    )
                        except Exception as e:
                            st.error(e)
                        else:
                            try:
                                os.remove(r'./fixed_errorneous_records.parquet')
                            except OSError:
                                pass
            else:
                st.error('Erroneous Records Not Found 😁')
        except Exception as e:
            st.error(e)
