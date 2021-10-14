

from botocore.retryhandler import EXCEPTION_MAP
import jaydebeapi
from numpy import log
import pandas as pd
import logging
import boto3
import fsspec
import s3fs
import yaml
from datetime import date, timedelta
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,csv_genarator, \
                     last_date_modified, stage_count,start_date,end_date,insert_to_audit,\
                     update_to_audit,error_to_audit,get_sf_connect,copy_command,etl_process_audit,\
                     etl_process_id,source_count,send_email,insert_file_log,stage_count,\
                     inc_filter_by,truncate

"""
    Gobal variables 
"""

transactions_object_name='Transactions'
transactions_format='csv'
transactions_file_date=incremental_date()
transactions_transaction_date=start_date()
transactions_end_date = end_date()
transactions_columns='columns'
transactions_query='query'
ns=get_config('NS')
transactions_ns_failed_subject=ns['constant']['ns_mail_failed']
transactions_ns_failed_content=ns['constant']['ns_failed_content']
transactions_ns_schema=ns['jdbc']['schema']
transactions_local_path=ns['jdbc']['local_path']
transactions_user=ns['sf']['user']
transactions_account=ns['sf']['account']
transactions_database=ns['sf']['database']
transactions_password=ns['sf']['password']
transactions_role=ns['sf']['role']
transactions_landingschema=ns['sf']['landingschema']
transactions_integartionschema=ns['sf']['integartionschema']
transactions_warehouse=ns['sf']['warehouse']
transactions_warehouse=ns['sf']['warehouse']
transactions_schema_audit=ns['sf']['schema_audit']
transactions_audit_table_name=ns['sf']['audit_table_name']
etl_table_name=ns['sf']['etl_table_name']

transactions_aws_path=ns['constant']['aws_path']
extraction_task=ns['constant']['extraction']
transactions_task=ns['constant']['task']
stage=ns['constant']['stage']
target=ns['constant']['target']
transactions_db_columns=get_db_config(objectname=transactions_object_name,source=transactions_columns)
transactions_column_p1=transactions_db_columns['phase_1']
transactions_column_p2=transactions_db_columns['phase_2']
transactions_column_p3=transactions_db_columns['phase_3']
transactions_db_query=get_db_config(objectname=transactions_object_name,source=transactions_query)
transactions_ns_query_P1=transactions_db_query['ns_phase_1']
transactions_ns_query_P2=transactions_db_query['ns_phase_2']
transactions_ns_query_P3=transactions_db_query['ns_phase_3']
transactions_truncate_p1=transactions_db_query['truncate_p1']
transactions_truncate_p2=transactions_db_query['truncate_p2']
transactions_truncate_p3=transactions_db_query['truncate_p3']
transactions_copy_p1=transactions_db_query['copy_command_p1']
transactions_copy_p2=transactions_db_query['copy_command_p2']
transactions_copy_p3=transactions_db_query['copy_command_p3']


transactions_extract_date=transactions_transaction_date
transactions_platform=ns["jdbc"]["platform"]
transactions_workflowname=transactions_object_name

transactions_nsquery_P1=transactions_ns_query_P1.format(table=transactions_object_name)
transactions_nsquery_P2=transactions_ns_query_P2.format(table=transactions_object_name)
transactions_nsquery_P3=transactions_ns_query_P3.format(table=transactions_object_name)
"""
    condition
"""
transactions_filter_condition=inc_filter_by(transactions_object_name)

transactions_lastdatemodified=transactions_filter_condition[0]
transactions_order_by=transactions_filter_condition[1]

transactionslastdatemodified=last_date_modified(object_name=transactions_object_name)
transactionslastdatemodified_P1=transactionslastdatemodified
transactionslastdatemodified_P2=transactionslastdatemodified
transactionslastdatemodified_P3=transactionslastdatemodified

transactions_query_P1=transactions_nsquery_P1 + f" where {transactions_lastdatemodified} >'{transactionslastdatemodified_P1}' order by {transactions_order_by}"
transactions_query_P2=transactions_nsquery_P2 + f" where {transactions_lastdatemodified} >'{transactionslastdatemodified_P2}' order by {transactions_order_by}"
transactions_query_P3=transactions_nsquery_P3 + f" where {transactions_lastdatemodified} >'{transactionslastdatemodified_P3}' order by {transactions_order_by}"

def transactions_extraction_phase_1():
    logging.info(f'{transactions_object_name}:extraction of data is started for phase 1')
    try:
        
        transactions_sf_conn=get_sf_connect()
        transactions_sf_cursor=transactions_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=transactions_platform,WORKFLOWNAME=transactions_object_name,STATUS='Running')
            logging.info(f'{transactions_object_name}:ETL_process id is generated')
            logging.info('ETL_process id is generated')
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e) 
            transactions_remark=str(e)+' at etl Process id'
            logging.info(f'{transactions_object_name}:ETL_process id is generation failed')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return

        try:
            transactions_process_id=etl_process_id(object_name=transactions_object_name)
            logging.info(f'{transactions_object_name}:',"etl_process_id",transactions_process_id)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+ ' at fetching etl process id '
            logging.info(f'{transactions_object_name}:Failed to fetch etl_process_id')
            error_to_audit(object=transactions_object_name,task=transactions_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            transactions_sourcecount=source_count(object_name=transactions_object_name,lastmodifieddate=transactionslastdatemodified)
            logging.info(f'{transactions_object_name}:',f'source_count for {transactions_object_name} is :',transactions_sourcecount)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark= str(e) +' at soure _count'
            logging.info(f'{transactions_object_name}:Failed at fecth source count')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=transactions_object_name,source_type='NetSuite_DataBase',target_type="S3",package=transactions_platform,status='RUNNING',PROCESS_ID=transactions_process_id,sourcecount=transactions_sourcecount)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e) + " at inserting audit table"
            logging.info(f'{transactions_object_name}:Failed to insert etl_audit_table')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        
        transactions_ns_conn  =con()
        transactions_cursor=transactions_ns_conn.cursor()
        transactions_file_name=f'{transactions_object_name}_P1_{transactions_file_date}.{transactions_format}'
        transactions_path=f'{transactions_aws_path}{transactions_object_name}/{transactions_file_name}'
        try:
            transactions_result1=fetching_data(transactions_cursor,transactions_query_P1)    
        except Exception as e:
                logging.info(f'{transactions_object_name}:',e)
                transactions_remark= str(e) + f' at extraction data  for {transactions_file_name}'
                error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
                logging.info(f'{transactions_object_name}:Failed at fetching data from source')
                transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at fetchuing data from source ",e=e)
                send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
                return
        try:
            csv_genarator(transactions_result1,transactions_column_p1,transactions_path)  
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark= str(e) + f' At csv genearation   for {transactions_file_name}'
            # error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            logging.info(f'{transactions_object_name}:Failed at file Generation and uploading to S3 ')    
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_df1=pd.DataFrame(transactions_result1,columns=transactions_column_p1 )
        transactions_total_records_count_P1=len(transactions_df1)
        logging.info(f'{transactions_object_name}:transactions_total_records_count_P1')
        try:
            insert_file_log(PROCESS_ID=transactions_process_id,package=transactions_platform,
                            object=transactions_object_name,FILE_NAME=transactions_file_name,
                            file_count=transactions_total_records_count_P1,
                            status="COMPLETED")
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+' at inserting file log'
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            logging.info(f'{transactions_object_name}:Failed to insert file details in ETL FILE LOG ')
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_remark=''
        try:
            update_to_audit(rows_insert=0,remark='',enddate=transactions_end_date,process_id=transactions_process_id)
            logging.info(f'{transactions_object_name}:,updated etl audit table is completed')
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+ " at Update audit table"
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
       
            

    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        transactions_remark=str(e)+ " at Extraction data from source"
        error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
        logging.info(f'{transactions_object_name}:,Failed at Extraction of data from source')
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return

        


def transactions_extraction_phase_2():
    logging.info(f'{transactions_object_name}:extraction of data is started for phase 2')
    try:
        transactions_sf_conn=get_sf_connect()
        transactions_sf_cursor=transactions_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=transactions_platform,WORKFLOWNAME=transactions_object_name,STATUS='Running')
            logging.info(f'{transactions_object_name}:ETL_process id is generated')
            logging.info('ETL_process id is generated')
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e) 
            transactions_remark=str(e)+' at etl Process id'
            logging.info(f'{transactions_object_name}:ETL_process id is generation failed')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return

        try:
            transactions_process_id=etl_process_id(object_name=transactions_object_name)
            logging.info(f'{transactions_object_name}:',"etl_process_id",transactions_process_id)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+ ' at fetching etl process id '
            logging.info(f'{transactions_object_name}:Failed to fetch etl_process_id')
            error_to_audit(object=transactions_object_name,task=transactions_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            transactions_sourcecount=source_count(object_name=transactions_object_name,lastmodifieddate=transactionslastdatemodified)
            logging.info(f'{transactions_object_name}:',f'source_count for {transactions_object_name} is :',transactions_sourcecount)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark= str(e) +' at soure _count'
            logging.info(f'{transactions_object_name}:Failed at fecth source count')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=transactions_object_name,source_type='NetSuite_DataBase',target_type="S3",package=transactions_platform,status='RUNNING',PROCESS_ID=transactions_process_id,sourcecount=transactions_sourcecount)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e) + " at inserting audit table"
            logging.info(f'{transactions_object_name}:Failed to insert etl_audit_table')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_ns_conn  =con()
        transactions_cursor=transactions_ns_conn.cursor()
        transactions_file_name=f'{transactions_object_name}_P2_{transactions_file_date}.{transactions_format}'
        transactions_path=f'{transactions_aws_path}{transactions_object_name}/{transactions_file_name}'
        try:
            transactions_result2=fetching_data(transactions_cursor,transactions_query_P2)    
        except Exception as e:
                logging.info(f'{transactions_object_name}:',e)
                transactions_remark= str(e) +f' at extraction data  for {transactions_file_name}'
                error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
                logging.info(f'{transactions_object_name}:Failed at fetching data from source')
                transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at fetchuing data from source ",e=e)
                send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
                return
        try:
            csv_genarator(result=transactions_result2,columns=transactions_column_p2,path=transactions_path)  
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark= str(e) + f' At csv genearation  for {transactions_file_name}'
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            logging.info(f'{transactions_object_name}:Failed at file Generation and uploading to S3 ')    
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_df2=pd.DataFrame(transactions_result2,columns=transactions_column_p2 )
        transactions_total_records_count_P2=len(transactions_df2)
        logging.info(f'{transactions_object_name}:',transactions_total_records_count_P2)
        try:
            insert_file_log(PROCESS_ID=transactions_process_id,package=transactions_platform,
                            object=transactions_object_name,FILE_NAME=transactions_file_name,
                            file_count=transactions_total_records_count_P2,
                            status="COMPLETED")
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+' at inserting file log'
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            logging.info(f'{transactions_object_name}:Failed to insert file deatils in ETL FILE LOG ')
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_remark=''
        try:
            update_to_audit(rows_insert=0,
                remark=transactions_remark,enddate=transactions_end_date,process_id=transactions_process_id)
            logging.info(f'{transactions_object_name}:,updated etl audit table is completed')
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+ " at Update audit table"
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
       
            

    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        transactions_remark=str(e)+ " at Extraction data from source"
        error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
        logging.info(f'{transactions_object_name}:,Failed at Extraction of data from source')
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return

def transactions_extraction_phase_3():
    logging.info(f'{transactions_object_name}:extraction of data is started for phase 3')
    try:
        
        transactions_sf_conn=get_sf_connect()
        transactions_sf_cursor=transactions_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=transactions_platform,WORKFLOWNAME=transactions_object_name,STATUS='Running')
            logging.info(f'{transactions_object_name}:ETL_process id is generated')
            logging.info('ETL_process id is generated')
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e) 
            transactions_remark=str(e)+' at etl Process id'
            logging.info(f'{transactions_object_name}:ETL_process id is generation failed')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return

        try:
            transactions_process_id=etl_process_id(object_name=transactions_object_name)
            logging.info(f'{transactions_object_name}:',"etl_process_id",transactions_process_id)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+ ' at fetching etl process id '
            logging.info(f'{transactions_object_name}:Failed to fetch etl_process_id')
            error_to_audit(object=transactions_object_name,task=transactions_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            transactions_sourcecount=source_count(object_name=transactions_object_name,lastmodifieddate=transactionslastdatemodified)
            logging.info(f'{transactions_object_name}:',f'source_count for {transactions_object_name} is :',transactions_sourcecount)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark= str(e) +' at soure _count'
            logging.info(f'{transactions_object_name}:Failed at fecth source count')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=transactions_object_name,source_type='NetSuite_DataBase',target_type="S3",package=transactions_platform,status='RUNNING',PROCESS_ID=transactions_process_id,sourcecount=transactions_sourcecount)
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e) + " at inserting audit table"
            logging.info(f'{transactions_object_name}:Failed to insert etl_audit_table')
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        
        transactions_ns_conn  =con()
        transactions_cursor=transactions_ns_conn.cursor()
        transactions_file_name=f'{transactions_object_name}_P3_{transactions_file_date}.{transactions_format}'
        transactions_path=f'{transactions_aws_path}{transactions_object_name}/{transactions_file_name}'
        try:
            transactions_result3=fetching_data(transactions_cursor,transactions_query_P3)    
        except Exception as e:
                logging.info(f'{transactions_object_name}:',e)
                transactions_remark= str(e) + f' at extraction data  for {transactions_file_name}'
                error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
                logging.info(f'{transactions_object_name}:Failed at fetchuing data from source')
                transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,
                                                                                    transaction_date=transactions_end_date,
                                                                                    job=" Failed at fetchuing data from source ",e=e)
                send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
                return
        try:
            csv_genarator(result=transactions_result3,columns=transactions_column_p3,path=transactions_path)  
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark= str(e) +f' At csv genearation   for {transactions_file_name}'
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            logging.info(f'{transactions_object_name}:Failed at file Generation and uploading to S3 ')    
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,
                                                                                transaction_date=transactions_end_date,
                                                                                job=" Failed at file Generation and uploading to S3 ",
                                                                                e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_df3=pd.DataFrame(transactions_result3,columns=transactions_column_p3 )
        transactions_total_records_count_P3=len(transactions_df3)
        logging.info(f'{transactions_object_name}:transactions_total_records_count_P3')
        try:
            insert_file_log(PROCESS_ID=transactions_process_id,package=transactions_platform,
                            object=transactions_object_name,FILE_NAME=transactions_file_name,
                            file_count=transactions_total_records_count_P3,status="COMPLETED")

        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+' at inserting file log'
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            logging.info(f'{transactions_object_name}:Failed to insert file deatils in ETL FILE LOG ')
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,
                                                                                transaction_date=transactions_end_date,
                                                                                job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return
        transactions_remark=""
        try:
            update_to_audit(rows_insert=0,remark=transactions_remark,
                            enddate=transactions_end_date,
                            process_id=transactions_process_id)
            logging.info(f'{transactions_object_name}:,updated etl audit table is completed')
        except Exception as e:
            logging.info(f'{transactions_object_name}:',e)
            transactions_remark=str(e)+ " at Update audit table"
            error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
            transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
            return      
            

    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        transactions_remark=str(e)+ " at Extraction data from source"
        error_to_audit(object=transactions_object_name,task=extraction_task,remark=transactions_remark)
        logging.info(f'{transactions_object_name}:,Failed at Extraction of data from source')
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=extraction_task,transaction_date=transactions_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return
        


def transactions_s3_sf():
    try:
        transactions_process_id=etl_process_id(object_name=transactions_object_name)
        logging.info(f'{transactions_object_name}:',"etl_process_id",transactions_process_id)
    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        transactions_remark=str(e)+ " at fetching etl processing id"
        error_to_audit(object=transactions_object_name,task=transactions_task,remark=transactions_remark)
        logging.info(f'{transactions_object_name}:Failed to fetch etl_process_id')
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=transactions_task,transaction_date=transactions_end_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return
    
    try:
        insert_to_audit(task=transactions_task,object=transactions_object_name,source_type=stage,target_type=target,package=transactions_platform,status='RUNNING',PROCESS_ID=transactions_process_id,sourcecount=0)
        logging.info(f'{transactions_object_name}:',"ETL_AUDIT_TABLE is inserted")
    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        transactions_remark= e+ 'at inserting audit table '
        logging.info(f'{transactions_object_name}:',"Failed to insert ETL_audit_table ")
        error_to_audit(object=transactions_object_name,task=transactions_task,remark=transactions_remark)
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=transactions_task,transaction_date=transactions_end_date,job="  Failed to insert ETL_audit_table  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return
    try:
           truncate(query=transactions_truncate_p1)
           truncate(query=transactions_truncate_p2)
           truncate(query=transactions_truncate_p3)
           logging.info('Truncate table is completed')
    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        remark=f'{transactions_object_name}_{transactions_file_date}'
        logging.info(f'{transactions_object_name}:Failed at truncating table  from  Landing schema/Table')
        error_to_audit(object=transactions_object_name,task=transactions_task,remark=remark)
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=transactions_task,transaction_date=transactions_end_date,job=f"  Falied at truncating Landing schema/Table for {remark}  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return
    transactions_j=1
    conn = get_sf_connect()
    cursor=conn.cursor()
    logging.info(copy_command)
    
    
    for copy_command_query in (transactions_copy_p1.format(filename=f'{transactions_object_name}_P1_{transactions_file_date}.csv',process_id=transactions_process_id),transactions_copy_p2.format(filename=f'{transactions_object_name}_P2_{transactions_file_date}.csv',process_id=transactions_process_id),transactions_copy_p3.format(filename=f'{transactions_object_name}_P3_{transactions_file_date}.csv',process_id=transactions_process_id)):      
            logging.info(copy_command_query)
            print(copy_command_query)
            try:
                cursor.execute(copy_command_query) 
                logging.info(f'{transactions_object_name}:',f'completd insertion of file to Snowflake database for phase_{transactions_j}')
                transactions_j+=1
                

            except Exception as e:
                
                logging.info(f'{transactions_object_name}:',e)
                transactions_remark=str(e)+ f'{transactions_object_name}_P{transactions_j}_{transactions_file_date}'
                logging.info(f'{transactions_object_name}:Failed at inserting data into Landing schema/Table')
                error_to_audit(object=transactions_object_name,transactions_task=transactions_task,remark=transactions_remark)
                transactions_ns_failed_constant=transactions_ns_failed_content.format(transactions_task=transactions_task,transaction_date=transactions_end_date,job=f"  Failed at inserting data into Landing schema/Table for {transactions_remark}  ",e=e)
                send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)                
                return
    
    
    try:
        update_to_audit(rows_insert=transactions_j-1,remark='',
                        enddate=transactions_end_date,process_id=transactions_process_id)
        logging.info(f'{transactions_object_name}:ETL_audit_table is updated')
    except Exception as e:
        logging.info(f'{transactions_object_name}:',e)
        logging.info(f'{transactions_object_name}:Failed to update ETL_audit_table')
        transactions_remark=str(e)+ 'At update audit table '
        error_to_audit(object=transactions_object_name,task=transactions_task,remark=transactions_remark)
        transactions_ns_failed_constant=transactions_ns_failed_content.format(task=transactions_task,transaction_date=transactions_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
        send_email(content=transactions_ns_failed_constant ,Subject=transactions_ns_failed_subject)
        return
