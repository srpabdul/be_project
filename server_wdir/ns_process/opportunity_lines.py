from datetime import date, timedelta
import logging
import pandas as pd
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,csv_genarator,\
                        start_date,end_date,insert_to_audit,update_to_audit,error_to_audit,\
                        get_sf_connect,copy_command,etl_process_audit,etl_process_id,source_count,\
                        send_email,insert_file_log,inc_filter_by,last_date_modified,truncate

"""
    Gobal variables 
"""

opr_ln_object_name='Opportunity_Lines'
opr_ln_format='csv'
opr_ln_file_date=incremental_date()
opr_ln_transaction_date=start_date()
opr_ln_end_date = end_date()
opr_ln_columns='phase'
opr_ln_query='query'
ns=get_config('NS')
opr_ln_ns_failed_subject=ns['constant']['ns_mail_failed']
opr_ln_ns_failed_content=ns['constant']['ns_failed_content']
opr_ln_ns_schema=ns['jdbc']['schema']
opr_ln_local_path=ns['jdbc']['local_path']
opr_ln_user=ns['sf']['user']
opr_ln_account=ns['sf']['account']
opr_ln_database=ns['sf']['database']
opr_ln_password=ns['sf']['password']
opr_ln_role=ns['sf']['role']
opr_ln_landingschema=ns['sf']['landingschema']
opr_ln_integartionschema=ns['sf']['integartionschema']
opr_ln_warehouse=ns['sf']['warehouse']
opr_ln_warehouse=ns['sf']['warehouse']
opr_ln_schema_audit=ns['sf']['schema_audit']
opr_ln_audit_table_name=ns['sf']['audit_table_name']
opr_ln_etl_table_name=ns['sf']['etl_table_name']
opr_ln_aws_path=ns['constant']['aws_path']
extraction_task=ns['constant']['extraction']
opr_ln_task=ns['constant']['task']
stage=ns['constant']['stage']
target=ns['constant']['target']

opr_ln_db_columns=get_db_config(objectname=opr_ln_object_name,source='columns')
opr_ln_column=opr_ln_db_columns['phase']
opr_ln_db_query=get_db_config(objectname=opr_ln_object_name,source='query')
opr_ln_ns_query=opr_ln_db_query['ns_query']
opr_ln_platform=ns["jdbc"]["platform"]
opr_ln_truncate=opr_ln_db_query['truncate']
opr_ln_copy_comand=opr_ln_db_query['copy_command']
opr_ln_workflowname=opr_ln_object_name
opr_ln_nsquery=opr_ln_ns_query.format(table=opr_ln_object_name)

"""
    condition
"""
opr_ln_filter_condition=inc_filter_by(opr_ln_object_name)

opr_ln_lastdatemodified=opr_ln_filter_condition[0]
opr_ln_order_by=opr_ln_filter_condition[1]

"""
    fecth date for extarction
"""
opr_ln_lastdatemodifieddate=last_date_modified(opr_ln_object_name)
logging.info(f'{opr_ln_object_name}:',opr_ln_lastdatemodifieddate)



opr_ln_nsquery=opr_ln_nsquery + f" where {opr_ln_lastdatemodified} >'{opr_ln_lastdatemodifieddate}' order by {opr_ln_order_by}"

def f():
    print(opr_ln_nsquery)

def opr_ln_extract_records():
    
    opr_ln_sf_conn=get_sf_connect()
    opr_ln_sf_cursor=opr_ln_sf_conn.cursor()
    """
        Generating ETL Process ID 
    """
    try:
        etl_process_audit(PLATFORM=opr_ln_platform,WORKFLOWNAME=opr_ln_object_name,STATUS='Running')
        logging.info(f'{opr_ln_object_name}:ETL_process id is generated')
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:ETL_process id is generation failed')
       
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" ETL_process id is generation failed ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    try:
        opr_ln_process_id=etl_process_id(object_name=opr_ln_object_name)
        logging.info(f'{opr_ln_object_name}:',"etl_process_id",opr_ln_process_id)
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:Failed to fetch etl_process_id')
        logging.info(f'{opr_ln_object_name}:Failed to fetch etl_process_id')
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Failed to fetch etl_process_id ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    
    """
        Taking Source count
    """
    try:
        opr_ln_sourcecount=source_count(object_name=opr_ln_object_name,lastmodifieddate=opr_ln_lastdatemodifieddate)
        logging.info(f'{opr_ln_object_name}:',f'source_count for {opr_ln_object_name} is :',opr_ln_sourcecount)
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:Failed at fecth source count')
        
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Failed at fecth source count ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    """
        Inserting Audit table 
    """
    try:
            insert_to_audit(task=extraction_task,object=opr_ln_object_name,source_type='NetSuite_DataBase',target_type="S3",package=opr_ln_platform,status='RUNNING',PROCESS_ID=opr_ln_process_id,sourcecount=opr_ln_sourcecount)
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:Failed to insert etl_audit_table')
       
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Failed to insert etl_audit_table ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    i=1
    try:
        opr_ln_ns_conn  =con()
        opr_ln_cursor=opr_ln_ns_conn.cursor()
        opr_ln_file_name=f'{opr_ln_object_name}_{opr_ln_file_date}.{opr_ln_format}'
        opr_ln_path=f'{opr_ln_aws_path}{opr_ln_object_name}/{opr_ln_file_name}'
        # path=f'E:/{object_name}/{file_name}'
        try:
            opr_ln_result=fetching_data(opr_ln_cursor,opr_ln_nsquery)
        except Exception as e:
            logging.info(f'{opr_ln_object_name}:',e)
            logging.info(f'{opr_ln_object_name}:Fialed at fetching data from source')
            # logging.info('Fialed at fetching data')
            opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Fialed at fetching data from source ",e=e)
            send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
            return
        try:
            csv_genarator(opr_ln_result,opr_ln_column,opr_ln_path)  
        except Exception as e:
            logging.info(f'{opr_ln_object_name}:Failed at file Generation and uploading to S3 ')    
          
            opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
            send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
            return
        opr_ln_df=pd.DataFrame(opr_ln_result,columns=opr_ln_column)
        opr_lin_count=len(opr_ln_df)
        logging.info(opr_lin_count)
        try:
            insert_file_log(PROCESS_ID=opr_ln_process_id,package=opr_ln_platform,object=opr_ln_object_name,
                            FILE_NAME=opr_ln_file_name,file_count=opr_lin_count,
                           status="COMPLETED")
        except Exception as e:
            logging.info(f'{opr_ln_object_name}:',e)
            logging.info(f'{opr_ln_object_name}:Failed to insert file deatils in ETL FILE LOG ')
            ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        
        opr_ln_remark=''
        try:
            update_to_audit(rows_insert=0,remark=opr_ln_remark,
                            enddate=opr_ln_end_date,process_id=opr_ln_process_id)
            logging.info(f'{opr_ln_object_name}:ETL Audit is updated sucessfully ')
            
        except Exception as e:
            logging.info(f'{opr_ln_object_name}:',e)
            logging.info(f'{opr_ln_object_name}:',"Failed to update ETL Audit table ")
            opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job=" Failed to update ETL Audit table  ",e=e)
            send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:Failed at Extraction of data from source')
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=extraction_task,transaction_date=opr_ln_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
def opr_ln_s3_sf():
    try:
        opr_ln_process_id=etl_process_id(object_name=opr_ln_object_name)
        logging.info(f'{opr_ln_object_name}:',"etl_process_id",opr_ln_process_id)
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:Failed to fetch etl_process_id')
   
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task='S3_Landind',transaction_date=opr_ln_end_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    
    try:
        insert_to_audit(task=opr_ln_task,object=opr_ln_object_name,source_type=stage,target_type="Snowflake",package=opr_ln_platform,status='RUNNING',PROCESS_ID=opr_ln_process_id,sourcecount=0)
        logging.info(f'{opr_ln_object_name}:',"ETL_AUDIT_TABLE is inserted")
      
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:',"Failed to insert ETL_audit_table ")
      
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=opr_ln_task,transaction_date=opr_ln_end_date,job="  Failed to insert ETL_audit_table  ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    
    try:
        truncate(query=opr_ln_truncate)
        logging.info('Truncate table is completed')
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        remark=f'{opr_ln_object_name}_{opr_ln_file_date}'
        logging.info(f'{opr_ln_object_name}:Failed at truncating table  from  Landing schema/Table')
        error_to_audit(object=opr_ln_object_name,task=opr_ln_task,remark=remark)
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=opr_ln_task,transaction_date=opr_ln_end_date,job=f"  Falied at truncating Landing schema/Table for {remark}  ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
  
    opr_ln_copy_query=opr_ln_copy_comand.format(filename=f'{opr_ln_object_name}_{opr_ln_file_date}.csv',process_id=opr_ln_process_id)
    logging.info(opr_ln_copy_query)
    try:
        logging.info('Executing copy command')
        copy_command(opr_ln_copy_query) 
        logging.info(f'{opr_ln_object_name}:',f'completd insertion of file to Snowflake database')
   
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        opr_ln_remark=f'{opr_ln_object_name}_{opr_ln_file_date}'
        logging.info(f'{opr_ln_object_name}:Failed at inserting data into Landing schema/Table')
        error_to_audit(object=opr_ln_object_name,task=opr_ln_task,remark=opr_ln_remark)
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=opr_ln_task,transaction_date=opr_ln_end_date,job=f"  Failed at inserting data into Landing schema/Table for {opr_ln_remark}  ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return
    try:
        update_to_audit(rows_insert=1,remark='',
                        enddate=opr_ln_end_date,process_id=opr_ln_process_id)
        logging.info(f'{opr_ln_object_name}:ETL_audit_table is updated')
    except Exception as e:
        logging.info(f'{opr_ln_object_name}:',e)
        logging.info(f'{opr_ln_object_name}:Failed to update ETL_audit_table')
       
        opr_ln_ns_failed_constant=opr_ln_ns_failed_content.format(task=opr_ln_task,transaction_date=opr_ln_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
        send_email(content=opr_ln_ns_failed_constant ,Subject=opr_ln_ns_failed_subject)
        return

