from datetime import date, timedelta
import logging
import pandas as pd
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,\
                     csv_genarator,start_date,end_date,insert_to_audit,update_to_audit,\
                     error_to_audit,get_sf_connect,copy_command,\
                     etl_process_id,source_count,send_email,insert_file_log,inc_filter_by,\
                     last_date_modified,truncate

"""
    Gobal variables 
"""

account_object_name='Accounts'
account_format='csv'
account_file_date=incremental_date()
account_transaction_date=start_date()
account_end_date = end_date()

ns=get_config('NS')
account_ns_failed_subject=ns['constant']['ns_mail_failed']
account_ns_failed_content=ns['constant']['ns_failed_content']
account_ns_schema=ns['jdbc']['schema']
account_local_path=ns['jdbc']['local_path']
account_user=ns['sf']['user']
account_account=ns['sf']['account']
account_database=ns['sf']['database']
account_password=ns['sf']['password']
account_role=ns['sf']['role']
account_landingschema=ns['sf']['landingschema']
account_integartionschema=ns['sf']['integartionschema']
account_warehouse=ns['sf']['warehouse']
account_warehouse=ns['sf']['warehouse']
account_schema_audit=ns['sf']['schema_audit']
account_audit_table_name=ns['sf']['audit_table_name']
account_etl_table_name=ns['sf']['etl_table_name']
account_aws_path=ns['constant']['aws_path']
account_task=ns['constant']['task']
extarction_task=ns['constant']['extraction']
stage=ns['constant']['stage']
target=ns['constant']['target']

account_db_columns=get_db_config(objectname=account_object_name,source='columns')
account_column=account_db_columns['phase']
account_db_query=get_db_config(objectname=account_object_name,source='query')
account_ns_query=account_db_query['ns_query']
account_platform=ns["jdbc"]["platform"]
account_workflowname=account_object_name
account_dbquery=account_ns_query.format(table=account_object_name)
account_truncate_query=account_db_query['truncate']
account_copycommand=account_db_query['copy_commad']

def check():
    print(account_truncate_query) 

"""
    condition
"""
account_filter_condition=inc_filter_by(account_object_name)
account_lastdatemodified=account_filter_condition[0]
account_order_by=account_filter_condition[1]
"""
    fecth date for extarction
"""

account_last_modified_date=last_date_modified(object_name=account_object_name)


accountsquery=account_dbquery + f" where {account_lastdatemodified} > '{account_last_modified_date}' order by {account_order_by}"
   

def account_extract_records():
    account_sf_conn=get_sf_connect()
    account_sf_cursor=account_sf_conn.cursor()
    """
         ETL Process ID 
    """
    
    try:
        account_process_id=etl_process_id(object_name=account_object_name)
        logging.info(f'{account_object_name}:',"etl_process_id",account_process_id)
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        logging.info(f'{account_object_name}:Failed to fetch etl_process_id')
        account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Failed to fetch etl_process_id ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    
    """
        Taking Source count
    """
    try:
        account_sourcecount=source_count(object_name=account_object_name,lastmodifieddate=account_last_modified_date)
        logging.info(f'{account_object_name}:',f'source_count for {account_object_name} is :',account_sourcecount)
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        logging.info(f'{account_object_name}:Failed at fecth source count')

        account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Failed at fecth source count ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    """
        Inserting Audit table 
    """
    try:
        insert_to_audit(task=extarction_task,object=account_object_name,source_type='NetSuite_DataBase',target_type="S3",package=account_platform,status='RUNNING',PROCESS_ID=account_process_id,sourcecount=account_sourcecount)
    except Exception as e:
        logging.info(f'{account_object_name}:Failed to insert etl_audit_table')

        account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Failed to insert etl_audit_table ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    i=1
    try:
        account_ns_conn  =con()
        account_cursor=account_ns_conn.cursor()
        account_file_name=f'{account_object_name}_{account_file_date}.{account_format}'
        account_path=f'{account_aws_path}{account_object_name}/{account_file_name}'
        # path=f'E:/{object_name}/{file_name}'
        
        try:
            logging.info(f'{account_object_name}:',accountsquery)
            account_result=fetching_data(account_cursor,accountsquery)
        except Exception as e:
            logging.info(f'{account_object_name}:',f'{account_object_name}:',e)
            logging.info(f'{account_object_name}:Failed at fetching data from source')
            # logging.info('Failed at fetching data')
            account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Failed at fetching data from source ",e=e)
            send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
            return
        try:
            
            csv_genarator(result=account_result,columns=account_column,path=account_path)  
        except Exception as e:
            print(e)
            print('Falied at file Generation and uploading to S3 ')    
            logging.info('Falied at file Generation and uploading to S3 ')
            account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Falied at file Generation and uploading to S3 ",e=e)
            send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
            return
        df=pd.DataFrame(account_result,columns=account_column )
        count=len(df)
        try:
            insert_file_log(PROCESS_ID=account_process_id,package=account_platform,object=account_object_name,FILE_NAME=account_file_name,file_count=count,status="COMPLETED")
        except  Exception as e:
            logging.info(f'{account_object_name}:',e)
            logging.info(f'{account_object_name}:Failed to insert file deatils in ETL FILE LOG ')
            
            account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        df=pd.DataFrame(account_result,columns=account_column)
        account_file_name==f'{account_object_name}_{account_file_date}.{account_format}'
        count_p1=len(df)
        logging.info(f'{account_object_name}:',account_file_name,count_p1)
        account_remark=''
        try:
            update_to_audit(rows_insert=0,remark=account_remark,
                            enddate=account_end_date,process_id=account_process_id)
            logging.info(f'{account_object_name}:ETL Audit is updated sucessfully ')
    
        except Exception as e:
            logging.info(f'{account_object_name}:',e)
            logging.info(f'{account_object_name}:',"Failed to update ETL Audit table ")
   
            account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job=" Failed to update ETL Audit table  ",e=e)
            send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        logging.info(f'{account_object_name}:Failed at Extraction of data from source')
        
        account_ns_failed_constant=account_ns_failed_content.format(task=extarction_task,transaction_date=account_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
def account_s3_sf():
    try:
        account_process_id=etl_process_id(object_name=account_object_name)
        logging.info(f'{account_object_name}:',"etl_process_id",account_process_id)
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        logging.info(f'{account_object_name}:Failed to fetch etl_process_id')
     
        account_ns_failed_constant=account_ns_failed_content.format(task=account_task,transaction_date=account_end_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    
    try:
        insert_to_audit(task=account_task,object=account_object_name,source_type=stage,target_type=target,package=account_platform,status='RUNNING',PROCESS_ID=account_process_id,sourcecount=0)
        logging.info(f'{account_object_name}:',"ETL_AUDIT_TABLE is inserted")
  
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        logging.info(f'{account_object_name}:',"Falied to insert ETL_audit_table ")
      
        account_ns_failed_constant=account_ns_failed_content.format(task=account_task,transaction_date=end_date,job="  Falied to insert ETL_audit_table  ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    try:
           truncate(query=account_truncate_query)
           logging.info('Truncate table is completed')
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        remark=f'{account_object_name}_{account_file_date}'
        logging.info(f'{account_object_name}:Failed at truncating table  from  Landing schema/Table')
        error_to_audit(object=account_object_name,task=account_task,remark=remark)
        account_ns_failed_constant=account_ns_failed_content.format(task=account_task,transaction_date=account_end_date,job=f"  Falied at truncating Landing schema/Table for {remark}  ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    account_file_name=f'{account_object_name}_{account_file_date}.csv'
    account_copy_command=account_copycommand.format(filename=account_file_name,process_id=account_process_id)

    try:            
        copy_command(account_copy_command) 
        logging.info(f'{account_object_name}:',f'Completd insertion of file to Snowflake database')
     
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        remark=f'{account_object_name}_{account_file_date}'
        logging.info(f'{account_object_name}:Failed at inserting data into Landing schema/Table')
        error_to_audit(object=account_object_name,task=account_task,remark=remark)
        account_ns_failed_constant=account_ns_failed_content.format(task=account_task,transaction_date=account_end_date,job=f"  Falied at inserting data into Landing schema/Table for {remark}  ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return
    
    try:
        update_to_audit(rows_insert=1,remark='',enddate=account_end_date,
                        process_id=account_process_id)
        logging.info(f'{account_object_name}:ETL_audit_table is updated')
    except Exception as e:
        logging.info(f'{account_object_name}:',e)
        logging.info(f'{account_object_name}:Failed to update ETL_audit_table')
     
        account_ns_failed_constant=account_ns_failed_content.format(task=account_task,transaction_date=account_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
        send_email(content=account_ns_failed_constant ,Subject=account_ns_failed_subject)
        return


