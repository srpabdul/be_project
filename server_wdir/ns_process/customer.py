
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
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,csv_genarator,stage_count,start_date,end_date,\
                    insert_to_audit,update_to_audit,error_to_audit,get_sf_connect,copy_command,etl_process_audit,etl_process_id,\
                    source_count,send_email,insert_file_log,stage_count,inc_filter_by,last_date_modified,\
                    truncate

"""
    Gobal variables 
"""


customers_object_name='Customers'
customers_format='csv'
customers_file_date=incremental_date()
customers_transaction_date=start_date()
customers_end_date = end_date()
customers_columns='columns'
customers_query='query'
ns=get_config('NS')

customers_ns_failed_subject=ns['constant']['ns_mail_failed']
customers_ns_failed_content=ns['constant']['ns_failed_content']

customers_ns_schema=ns['jdbc']['schema']
customers_local_path=ns['jdbc']['local_path']

customers_user=ns['sf']['user']
customers_customers=ns['sf']['account']
customers_database=ns['sf']['database']
customers_password=ns['sf']['password']
customers_role=ns['sf']['role']
customers_landingschema=ns['sf']['landingschema']
customers_integartionschema=ns['sf']['integartionschema']
customers_warehouse=ns['sf']['warehouse']
customers_warehouse=ns['sf']['warehouse']
customers_schema_audit=ns['sf']['schema_audit']
customers_audit_table_name=ns['sf']['audit_table_name']
customers_etl_table_name=ns['sf']['etl_table_name']

customers_aws_path=ns['constant']['aws_path']
extraction_task=ns['constant']['extraction']
stage=ns['constant']['stage']
target=ns['constant']['target']
customers_task=ns['constant']['task']

customers_db_columns=get_db_config(objectname=customers_object_name,source=customers_columns)
customers_column_p1=customers_db_columns['phase_1']
customers_column_p2=customers_db_columns['phase_2']
customers_db_query=get_db_config(objectname=customers_object_name,source=customers_query)
customers_ns_queryP1=customers_db_query['ns_phase_1']
customers_ns_queryP2=customers_db_query['ns_phase_2']
customers_ns_customers_query_P1=customers_ns_queryP1.format(table=customers_object_name)
customers_ns_customers_query_P2=customers_ns_queryP2.format(table=customers_object_name)
customers_extract_date=customers_transaction_date
customers_platform=ns["jdbc"]["platform"]
customers_workflowname=customers_object_name
customers_truncate_P1=customers_db_query['truncate_1']
customers_truncate_P2=customers_db_query['truncate_2']
customer_copy_p1=customers_db_query['copy_command_p1']
customer_copy_p2=customers_db_query['copy_command_p2']
"""
    condition
"""
customers_filter_condition=inc_filter_by(object_name=customers_object_name)

customers_lastdatemodified=customers_filter_condition[0]
customers_order_by=customers_filter_condition[1]

customers_lastmodified_date= "2021-10-07 10:21:29" #last_date_modified(object_name=customers_object_name)


customers_query_p1=customers_ns_customers_query_P1 + f"  where {customers_lastdatemodified} >='{ customers_lastmodified_date }' order by CUSTOMER_ID"
customers_query_p2=customers_ns_customers_query_P2 + f"  where {customers_lastdatemodified} >='{ customers_lastmodified_date }' order by CUSTOMER_ID"

def customers_extraction_phase_1():
    logging.info(f'{customers_object_name}:', 'Extraction of data is started for phase 1')
    try:
        
        customers_sf_conn=get_sf_connect()
        customers_sf_cursor=customers_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=customers_platform,WORKFLOWNAME=customers_object_name,STATUS='Running')
            logging.info(f'{customers_object_name} :','ETL_process id is generated')
        except Exception as e:
            logging.info(f'{customers_object_name} :', e) 
            customers_remark=str(e)  +' at etl Process id'
            logging.info(f'{customers_object_name} :','ETL_process id is generation failed')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return

        try:
            customers_process_id=etl_process_id(object_name=customers_object_name)
            logging.info(f'{customers_object_name} etl_process_id :',customers_process_id)
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=e+ ' at fetching etl process id '
            logging.info(f'{customers_object_name}:',' Failed to fetch etl_process_id')
            error_to_audit(object=customers_object_name,task=customers_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            customers_sourcecount=source_count(object_name=customers_object_name,lastmodifieddate=customers_lastmodified_date)
            logging.info(f'source_count for {customers_object_name} is :',customers_sourcecount)
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark= str(e) + ' at soure _count'
            logging.info(f'{customers_object_name}:',' Failed at fecth source count')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=customers_object_name,source_type='NetSuite_DataBase',target_type="S3",package=customers_platform,status='RUNNING',PROCESS_ID=customers_process_id,sourcecount=customers_sourcecount)
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e) + " at inserting audit table"
            logging.info(f'{customers_object_name}:',' Failed to insert etl_audit_table')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return 
        customers_ns_conn  =con()
        customers_cursor=customers_ns_conn.cursor()
        customers_file_name=f'{customers_object_name}_P1_{customers_file_date}.{customers_format}'
        customers_path=f'{customers_aws_path}{customers_object_name}/{customers_file_name}'
        try:
            customers_result1=fetching_data(customers_cursor,customers_query_p1) 
            logging.info(f'{customers_object_name}:','fetching_data records')
        except Exception as e:
                logging.info(f'{customers_object_name}:  e')
                customers_remark= str(e)  + f' at extraction data  for {customers_file_name}'
                error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
                logging.info(f'{customers_object_name}:','failed at fetchuing data from source')
                customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" failed at fetchuing data from source ",e=e)
                send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
                return
        try:
            csv_genarator(customers_result1,customers_column_p1,customers_path)  
        except Exception as e:
            logging.info(f'{customers_object_name}:',e)
            customers_remark= str(e)  + f' At csv genearation   for {customers_file_name}'
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name}:','Falied at file Generation and uploading to S3 ')    
            logging.info('Falied at file Generation and uploading to S3 ')
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Falied at file Generation and uploading to S3 ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        customers_df=pd.DataFrame(customers_result1,columns=customers_column_p1 )
        customers_total_records_count_P1=len(customers_df)
        logging.info(f'{customers_object_name}:', customers_total_records_count_P1)
        try:
            insert_file_log(PROCESS_ID=customers_process_id,package=customers_platform,object=customers_object_name,FILE_NAME=customers_file_name,file_count=customers_total_records_count_P1,status="COMPLETED")
        except Exception as e:
            logging.info(f'{customers_object_name}:',e)
            customers_remark=str(e)  +' at inserting file log'
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name}:',' Failed to insert file deatils in ETL FILE LOG ')
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
        try:
            update_to_audit(rows_insert=0,remark="",enddate=customers_end_date,
                            process_id=customers_process_id)
            logging.info(f'{customers_object_name}:','updated etl audit table is completed')
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e)  + " at Update audit tablr"
            error_to_audit(object=customers_object_name,task=extraction_task,remark="")
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
    except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e)  + " at extracion data from source"
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name}:',' Failed at Extraction of data from source')
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
            


def customers_extraction_phase_2():
    logging.info(f'{customers_object_name}:', 'Extraction of data is started for phase 1')
    try:
        
        customers_sf_conn=get_sf_connect()
        customers_sf_cursor=customers_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=customers_platform,WORKFLOWNAME=customers_object_name,STATUS='Running')
            logging.info(f'{customers_object_name} :','ETL_process id is generated')
        except Exception as e:
            logging.info(f'{customers_object_name} :', e) 
            customers_remark=str(e)  +' at etl Process id'
            logging.info(f'{customers_object_name} :','ETL_process id is generation failed')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return

        try:
            customers_process_id=etl_process_id(object_name=customers_object_name)
            logging.info(f'{customers_object_name} etl_process_id :',customers_process_id)
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=e+ ' at fetching etl process id '
            logging.info(f'{customers_object_name}:',' Failed to fetch etl_process_id')
            error_to_audit(object=customers_object_name,task=customers_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            customers_sourcecount=source_count(object_name=customers_object_name,lastmodifieddate=customers_lastmodified_date)
            logging.info(f'source_count for {customers_object_name} is :',customers_sourcecount)
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark= str(e) + ' at soure _count'
            logging.info(f'{customers_object_name}:',' Failed at fecth source count')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=customers_object_name,source_type='NetSuite_DataBase',target_type="S3",package=customers_platform,status='RUNNING',PROCESS_ID=customers_process_id,sourcecount=customers_sourcecount)
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e) + " at inserting audit table"
            logging.info(f'{customers_object_name}:',' Failed to insert etl_audit_table')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return 
        
        try:
            customers_process_id=etl_process_id(object_name=customers_object_name)
            logging.info(f'{customers_object_name}_etl_process_id',customers_process_id)
        except Exception as e:
            logging.info(f'{customers_object_name}:',e)
            customers_remark=e+ ' at fetching etl process id '
            logging.info(f'{customers_object_name}:','Failed to fetch etl_process_id')
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        customers_ns_conn  =con()
        customers_cursor=customers_ns_conn.cursor()
        customers_file_name=f'{customers_object_name}_P2_{customers_file_date}.{customers_format}'
        customers_path=f'{customers_aws_path}{customers_object_name}/{customers_file_name}'
        try:
            customers_result2=fetching_data(customers_cursor,customers_query_p2)    
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark= str(e)  + f' at extraction data  for {customers_file_name}'
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name} : Failed at fetchting data from source')
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" failed at fetchuing data from source ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        try:
            csv_genarator(customers_result2,customers_column_p2,customers_path)  
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark= str(e)  + f' At csv genearation   for {customers_file_name}'
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name} : Falied at file Generation and uploading to S3 ')    
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Falied at file Generation and uploading to S3 ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
        customers_df=pd.DataFrame(customers_result2,columns=customers_column_p2 )
        customers_total_records_count_P2=len(customers_df)
        logging.info(f'{customers_object_name} :',customers_total_records_count_P2)
        try:
            insert_file_log(PROCESS_ID=customers_process_id,package=customers_platform,object=customers_object_name,FILE_NAME=customers_file_name,file_count=customers_total_records_count_P2,status="COMPLETED")
        except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e)  +' at inserting file log'
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name}:',' Failed to insert file deatils in ETL FILE LOG ')
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
    except Exception as e:
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e)  + " at extracion data from source"
            error_to_audit(object=customers_object_name,task=extraction_task,remark=customers_remark)
            logging.info(f'{customers_object_name}:',' Failed at Extraction of data from source')
            customers_ns_failed_constant=customers_ns_failed_content.format(task=extraction_task,transaction_date=customers_end_date,job="  Failed at Extraction of data from source  ",e=e)
            send_email(content=customers_ns_failed_constant  ,Subject=customers_ns_failed_subject)
            return
    update_to_audit(rows_insert=0,remark='',
                    enddate=customers_end_date,
                    process_id=customers_process_id)            


def customers_s3_sf():
    
    try:
        customers_process_id=etl_process_id(object_name=customers_object_name)
        logging.info(f'{customers_object_name} etl_process_id :',customers_process_id)
    except Exception as e:
        logging.info(f'{customers_object_name} :', e)
        customers_remark=e+ " at fetching etl processing id"
        error_to_audit(object=customers_object_name,task=customers_task,remark=customers_remark)
        logging.info(f'{customers_object_name}:',' Failed to fetch etl_process_id')
        customers_ns_failed_constant=customers_ns_failed_content.format(task=customers_task,transaction_date=customers_end_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
        return
    
    try:
        insert_to_audit(task=customers_task,object=customers_object_name,source_type=stage,target_type=target,package=customers_platform,status='RUNNING',PROCESS_ID=customers_process_id,sourcecount=0)
        logging.info(f'{customers_object_name}:',"ETL_AUDIT_TABLE is inserted")
        logging.info("ETL_AUDIT_TABLE is inserted")
    except Exception as e:
        logging.info(f'{customers_object_name} :', e)
        customers_remark= e+ 'at inserting audit table '
        logging.info(f'{customers_object_name}:',"Falied to insert ETL_audit_table ")
        error_to_audit(object=customers_object_name,task=customers_task,remark=customers_remark)
        customers_ns_failed_constant=customers_ns_failed_content.format(task=customers_task,transaction_date=customers_end_date,job="  Falied to insert ETL_audit_table  ",e=e)
        send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
        return
    try:
           truncate(query=customers_truncate_P1)
           truncate(query=customers_truncate_P2)
           logging.info('Truncate table is completed')
    except Exception as e:
        logging.info(f'{customers_object_name}:',e)
        remark=f'{customers_object_name}_{customers_file_date}'
        logging.info(f'{customers_object_name}:Failed at truncating table  from  Landing schema/Table')
        error_to_audit(object=customers_object_name,task=customers_task,remark=remark)
        customers_ns_failed_constant=customers_ns_failed_content.format(task=customers_task,transaction_date=customers_end_date,job=f"  Falied at truncating Landing schema/Table for {remark}  ",e=e)
        send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
        return    

    customers_j=1
    
    try:
        for copy_comand_query in (customer_copy_p1.format(filename=f'{customers_object_name}_P1_{customers_file_date}.csv',process_id=customers_process_id),customer_copy_p2.format(filename=f'{customers_object_name}_P2_{customers_file_date}.csv',process_id=customers_process_id)):
            logging.info(copy_comand_query)
            copy_command(copy_comand_query)     
            logging.info(f'{customers_object_name}',f'completd insertion of file to Snowflake database customer phase 1' )
            customers_j+=1 
    except Exception as e:
            logging.info(f'{customers_object_name}_P{ customers_j }_{customers_file_date}')
            logging.info(f'{customers_object_name} :', e)
            customers_remark=str(e)  + f'{customers_object_name}_P{ customers_j }_{customers_file_date}'
            logging.info(f'{customers_object_name} : Falied at inserting data into Landing schema/Table')
            error_to_audit(object=customers_object_name,task=customers_task,remark=customers_remark)
            customers_ns_failed_constant=customers_ns_failed_content.format(task=customers_task,transaction_date=customers_end_date,job=f"  Falied at inserting data into Landing schema/Table for {customers_remark}  ",e=e)
            send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
            return
    
    try:
        update_to_audit(rows_insert=customers_j-1,remark='',
                        enddate=customers_end_date,
                        process_id=customers_process_id)
        logging.info(f'{customers_object_name}:','ETL_audit_table is updated')
    except Exception as e:
        logging.info(f'{customers_object_name} :', e)
        logging.info(f'{customers_object_name}:',' Failed to update ETL_audit_table')
        customers_remark=str(e)  + 'At update audit table '
        error_to_audit(object=customers_object_name,task=customers_task,remark=customers_remark)
        customers_ns_failed_constant=customers_ns_failed_content.format(task=customers_task,transaction_date=customers_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
        send_email(content=customers_ns_failed_constant ,Subject=customers_ns_failed_subject)
        return

