

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
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,\
                    csv_genarator, stage_count,start_date,end_date,insert_to_audit,\
                    update_to_audit,error_to_audit,get_sf_connect,copy_command,etl_process_audit,\
                    etl_process_id,source_count,send_email,insert_file_log,inc_filter_by,\
                    last_date_modified,truncate

"""
    Gobal variables 
"""

items_object_name='Items'
items_format='csv'
items_file_date=incremental_date()
items_transaction_date=start_date()
items_end_date = end_date()
items_columns='columns'
items_query='query'
ns=get_config('NS')
items_ns_failed_subject=ns['constant']['ns_mail_failed']
items_ns_failed_content=ns['constant']['ns_failed_content']
items_ns_schema=ns['jdbc']['schema']
items_local_path=ns['jdbc']['local_path']

items_user=ns['sf']['user']
items_account=ns['sf']['account']
items_database=ns['sf']['database']
items_password=ns['sf']['password']
items_role=ns['sf']['role']
items_landingschema=ns['sf']['landingschema']
items_integartionschema=ns['sf']['integartionschema']
items_warehouse=ns['sf']['warehouse']
items_warehouse=ns['sf']['warehouse']
items_schema_audit=ns['sf']['schema_audit']
items_audit_table_name=ns['sf']['audit_table_name']
items_etl_table_name=ns['sf']['etl_table_name']
items_aws_path=ns['constant']['aws_path']
extraction_task=ns['constant']['extraction']

stage=ns['constant']['stage']
target=ns['constant']['target']
task=ns['constant']['task']

items_db_columns=get_db_config(objectname=items_object_name,source=items_columns)
items_column_p1=items_db_columns['phase_1']
items_column_p2=items_db_columns['phase_2']
items_db_query=get_db_config(objectname=items_object_name,source=items_query)
items_ns_queryP1=items_db_query['ns_phase_1']
items_ns_queryP2=items_db_query['ns_phase_2']
items_ns_items_query_P1=items_ns_queryP1.format(table=items_object_name)
items_ns_items_query_P2=items_ns_queryP2.format(table=items_object_name)
items_items_extract_date=items_transaction_date
items_platform=ns["jdbc"]["platform"]
items_workflowname=items_object_name

items_truncate_p1=items_db_query['truncate_p1']
items_truncate_p2=items_db_query['truncate_p2']

items_copy_command_p1=items_db_query['copy_command_p1']
items_copy_command_p2=items_db_query['copy_command_p2']


"""
    condition
"""
items_filter_condition=inc_filter_by(items_object_name)
items_lastdatemodified=items_filter_condition[0]
items_order_by=items_filter_condition[1]


items_lastmodifieddate=last_date_modified(object_name=items_object_name)
items_lastmodifieddate_P1=items_lastmodifieddate
items_lastmodifieddate_P2=items_lastmodifieddate




items_query_p1=items_ns_items_query_P1 + f" where   {items_lastdatemodified} >'{items_lastmodifieddate_P1}' order by {items_order_by} "
items_query_p2=items_ns_items_query_P2 + f"  where  {items_lastdatemodified} >'{items_lastmodifieddate_P2}' order by {items_order_by} "

logging.info(f'{items_object_name}:',items_query_p1)
logging.info(f'{items_object_name}:',items_query_p2)


def items_extraction_phase_1():
    
    logging.info(f'{items_object_name}:extraction of data is started for phase 1')
    try:
        
        items_sf_conn=get_sf_connect()
        items_sf_cursor=items_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=items_platform,WORKFLOWNAME=items_object_name,STATUS='Running')
            logging.info(f'{items_object_name}:ETL_process id is generated')
    
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e) 
            items_remark=str(e)  +' at etl Process id'
            logging.info(f'{items_object_name}:ETL_process id is generation failed')
   
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return

        try:
            items_process_id=etl_process_id(object_name=items_object_name)
            logging.info(f'{items_object_name}:',f'{items_object_name}  etl_process_id',items_process_id)
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=e+ ' at fetching etl process id '
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed to fetch etl_process_id')
            error_to_audit(object=items_object_name,task=task,remark=items_remark)

            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            items_sourcecount=source_count(object_name=items_object_name,lastmodifieddate=items_lastmodifieddate)
            logging.info(f'{items_object_name}:',f'source_count for {items_object_name} is :',items_sourcecount)
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark= str(e) + ' at soure _count'
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed at fecth source count')
       
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=items_object_name,source_type='NetSuite_DataBase',target_type="S3",package="NetSuite",status='RUNNING',PROCESS_ID=items_process_id,sourcecount=items_sourcecount)
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e) + " at inserting audit table"
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed to insert etl_audit_table')
            
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return

        items_ns_conn  =con()
        items_cursor=items_ns_conn.cursor()
        items_file_name=f'{items_object_name}_P1_{items_file_date}.{items_format}'
        items_path=f'{items_aws_path}{items_object_name}/{items_file_name}'
        try:
            items_result1=fetching_data(items_cursor,items_query_p1)    
        except Exception as e:
                logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
                items_remark= str(e)  + f' at extraction data  for {items_file_name}'
                error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
                logging.info(f'{items_object_name}:',f'{items_object_name} Failed at fetching data from source')
           
                items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed at fetchuing data from source ",e=e)
                send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
                return
        try:
            csv_genarator(items_result1,items_column_p1,items_path)  
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark= str(e)  + f' At csv genearation   for {items_file_name}'
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed at file Generation and uploading to S3 ')    
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        items_df=pd.DataFrame(items_result1,columns=items_column_p1 )
        items_total_records_count_P1=len(items_df)
        logging.info(f'{items_object_name}:',items_total_records_count_P1)
        try:
            insert_file_log(PROCESS_ID=items_process_id,package=items_platform,object=items_object_name,FILE_NAME=items_file_name,file_count=items_total_records_count_P1,status='COMPLETED')
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e)  +' at inserting file log'
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed to insert file deatils in ETL FILE LOG ')
            
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        items_remark=" "
    

        try:
            update_to_audit(rows_insert=0,remark=items_remark,
                            enddate=items_end_date,process_id=items_process_id)
            logging.info(f'{items_object_name}:updated etl audit table is completed')
       
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e)  + " at Update audit table"
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
       
            

    except Exception as e:
        logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
        items_remark=str(e)  + " at Extraction data from source"
        error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
        logging.info(f'{items_object_name}:',f'{items_object_name} Failed at Extraction of data from source')
       
        items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        return


def items_extraction_phase_2():
    logging.info(f'{items_object_name}:extraction of data is started for phase 2')
    try:
        
        items_sf_conn=get_sf_connect()
        items_sf_cursor=items_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=items_platform,WORKFLOWNAME=items_object_name,STATUS='Running')
            logging.info(f'{items_object_name}:ETL_process id is generated')
    
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e) 
            items_remark=str(e)  +' at etl Process id'
            logging.info(f'{items_object_name}:ETL_process id is generation failed')
   
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return

        try:
            items_process_id=etl_process_id(object_name=items_object_name)
            logging.info(f'{items_object_name}:',f'{items_object_name}  etl_process_id',items_process_id)
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=e+ ' at fetching etl process id '
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed to fetch etl_process_id')
            error_to_audit(object=items_object_name,task=task,remark=items_remark)

            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            items_sourcecount=source_count(object_name=items_object_name,lastmodifieddate=items_lastmodifieddate)
            logging.info(f'{items_object_name}:',f'source_count for {items_object_name} is :',items_sourcecount)
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark= str(e) + ' at soure _count'
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed at fecth source count')
       
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=items_object_name,source_type='NetSuite_DataBase',target_type="S3",package="NetSuite",status='RUNNING',PROCESS_ID=items_process_id,sourcecount=items_sourcecount)
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e) + " at inserting audit table"
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed to insert etl_audit_table')
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        items_ns_conn  =con()
        items_cursor=items_ns_conn.cursor()
        items_file_name=f'{items_object_name}_P2_{items_file_date}.{items_format}'
        items_path=f'{items_aws_path}{items_object_name}/{items_file_name}'
        try:
            items_result2=fetching_data(items_cursor,items_query_p2)    
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark= str(e)  + f' at extraction data  for {items_file_name}'
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed at fetching data from source')
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed at fetchuing data from source ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        try:
            csv_genarator(items_result2,items_column_p2,items_path)  
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark= str(e)  + f' At csv genearation   for {items_file_name}'
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed at file Generation and uploading to S3 ')    
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            return
        items_df2=pd.DataFrame(items_result2,columns=items_column_p2 )
        items_total_records_count_P2=len(items_df2)
        logging.info(f'{items_object_name}:',items_total_records_count_P2)
        try:
            insert_file_log(PROCESS_ID=items_process_id,package=items_platform,object=items_object_name,FILE_NAME=items_file_name,file_count=items_total_records_count_P2,status="COMPLETED")
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e)  +' at inserting file log'
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed to insert file deatils in ETL FILE LOG ')
            
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            
        items_remark=""
            


        try:
            update_to_audit(rows_insert=0,remark=items_remark,
                            enddate=items_end_date,process_id=items_process_id)
            logging.info(f'{items_object_name}:updated etl audit table is completed')
       
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e)  + " at Update audit table"
            error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
       
            

    except Exception as e:
        logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
        items_remark=str(e)  + " at Extraction data from source"
        error_to_audit(object=items_object_name,task='Extarction',remark=items_remark)
        logging.info(f'{items_object_name}:',f'{items_object_name} Failed at Extraction of data from source')
       
        items_ns_failed_constant=items_ns_failed_content.format(task=extraction_task,transaction_date=items_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        return

def items_s3_sf():
    try:
        items_process_id=etl_process_id(object_name=items_object_name)
        logging.info(f'{items_object_name}:',f'{items_object_name}  etl_process_id',items_process_id)
    except Exception as e:
        logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
        items_remark=e+ " at fetching etl processing id"
        error_to_audit(object=items_object_name,task=task,remark=items_remark)
        logging.info(f'{items_object_name}:',f'{items_object_name} Failed to fetch etl_process_id')
       
        items_ns_failed_constant=items_ns_failed_content.format(task=task,transaction_date=items_end_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        return
    items_task=task
    try:
        insert_to_audit(task=items_task,object=items_object_name,source_type=stage,target_type=target,package="NetSuite",status='RUNNING',PROCESS_ID=items_process_id,sourcecount=0)
        logging.info(f'{items_object_name}:',"ETL_AUDIT_TABLE is inserted")
       
    except Exception as e:
        logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
        items_remark= e+ 'at inserting audit table '
        logging.info(f'{items_object_name}:',f'{items_object_name} Failed to insert ETL_audit_table ')
        error_to_audit(object=items_object_name,task=task,remark=items_remark)
       
        items_ns_failed_constant=items_ns_failed_content.format(task=items_task,transaction_date=items_end_date,job="  Failed to insert ETL_audit_table  ",e=e)
        send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        return
    try:
        truncate(query=items_truncate_p1)
        truncate(query=items_truncate_p2)
        logging.info('Truncate table is completed')
    except Exception as e:
        logging.info(f'{items_object_name}:',e)
        remark=f'{items_object_name}_{items_file_date}'
        logging.info(f'{items_object_name}:Failed at truncating table  from  Landing schema/Table')
        error_to_audit(object=items_object_name,task=items_task,remark=remark)
        items_ns_failed_constant=items_ns_failed_content.format(task=items_task,transaction_date=items_end_date,job=f"  Falied at truncating Landing schema/Table for {remark}  ",e=e)
        send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        return
    
    items_j=1
    for copy_command_query in (items_copy_command_p1.format(filename=f'{items_object_name}_P1_{items_file_date}.csv',process_id=items_process_id),items_copy_command_p2.format(filename=f'{items_object_name}_P2_{items_file_date}.csv',process_id=items_process_id)):          
        try: 
            print(copy_command_query)
            copy_command(copy_command_query) 
            items_j+=1
            logging.info(f'{items_object_name}:',f'completd insertion of file to Snowflake database for items_phase_1')
        except Exception as e:
            logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
            items_remark=str(e)  + f'{items_object_name}_P{items_j}_{items_file_date}'
            logging.info(f'{items_object_name}:',f'{items_object_name} Failed at inserting data into Landing schema/Table')
            error_to_audit(object=items_object_name,task=task,remark=items_remark)
            items_ns_failed_constant=items_ns_failed_content.format(task=items_task,transaction_date=items_end_date,job=f"  Failed at inserting data into Landing schema/Table for {items_remark}  ",e=e)
            send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
            break
  
    try:
        update_to_audit(rows_insert=items_j-1,remark='',
                        enddate=items_end_date,process_id=items_process_id)
        logging.info(f'{items_object_name}:ETL_audit_table is updated')
        
    except Exception as e:
        logging.info(f'{items_object_name}:',f'{items_object_name} :  ', e)
        logging.info(f'{items_object_name}:',f'{items_object_name} Failed to update ETL_audit_table')
        items_remark=str(e)  + 'At update audit table '
        error_to_audit(object=items_object_name,task=task,remark=items_remark)
        
        items_ns_failed_constant=items_ns_failed_content.format(task=items_task,transaction_date=items_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
        send_email(content=items_ns_failed_constant ,Subject=items_ns_failed_subject)
        return

