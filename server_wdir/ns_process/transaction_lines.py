from datetime import date, timedelta
import logging
import pandas as pd
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,csv_genarator,\
                        start_date,end_date,insert_to_audit,update_to_audit,error_to_audit,\
                        get_sf_connect,copy_command,etl_process_audit,etl_process_id,source_count,\
                        send_email,insert_file_log,inc_filter_by,last_date_modified


"""
    Gobal variables 
"""

trn_ln_object_name='Transaction_Lines'
trn_lnformat='csv'
trn_lnfile_date=incremental_date()
trn_lntransaction_date=start_date()
trn_lnend_date = end_date()
trn_lncolumns='phase'
trn_lnquery='query'
ns=get_config('NS')
trn_lnns_failed_subject=ns['constant']['ns_mail_failed']
trn_lnns_failed_content=ns['constant']['ns_failed_content']
trn_lnns_schema=ns['jdbc']['schema']
trn_lnlocal_path=ns['jdbc']['local_path']

trn_lnuser=ns['sf']['user']
trn_lnaccount=ns['sf']['account']
trn_lndatabase=ns['sf']['database']
trn_lnpassword=ns['sf']['password']
trn_lnrole=ns['sf']['role']
trn_lnlandingschema=ns['sf']['landingschema']
trn_lnintegartionschema=ns['sf']['integartionschema']
trn_lnwarehouse=ns['sf']['warehouse']
trn_lnwarehouse=ns['sf']['warehouse']
trn_lnschema_audit=ns['sf']['schema_audit']
trn_lnaudit_table_name=ns['sf']['audit_table_name']
trn_lnetl_table_name=ns['sf']['etl_table_name']

trn_lnaws_path=ns['constant']['aws_path']
extraction_task=ns['constant']['extraction']
trn_lntask=ns['constant']['task']
stage=ns['constant']['stage']
target=ns['constant']['target']



trn_lndb_columns=get_db_config(objectname=trn_ln_object_name,source='columns')
trn_lncolumn=trn_lndb_columns['phase']
trn_lndb_query=get_db_config(objectname=trn_ln_object_name,source='query')
trn_lnns_query=trn_lndb_query['ns_query']
trn_ln_truncate=trn_lndb_query['truncate']
trn_ln_copy_command=trn_lndb_query['copy_command']


trn_lnplatform=ns["jdbc"]["platform"]
trn_lnworkflowname=trn_ln_object_name

trn_lnnsquery=trn_lnns_query.format(table=trn_ln_object_name)

"""
    condition
"""
trn_lnfilter_condition=inc_filter_by(trn_ln_object_name)
logging.info(f'{trn_ln_object_name}:',trn_lnfilter_condition)


trn_ln_lastdatemodified=trn_lnfilter_condition[0]
trn_lnorder_by=trn_lnfilter_condition[1]


"""
    fecth date for extraction
"""
# def trn_lnlast_date_modified():
#     trn_lnsf_conn=get_sf_connect()
#     trn_lnsf_cursor=trn_lnsf_conn.cursor()
#     trn_lnquery_inc=f"""select replace(max({trn_ln_lastdatemodified}),'.000','') from "{trn_lndatabase}"."{trn_lnintegartionschema}"."NS_{trn_ln_object_name.upper()}" """
#     print(trn_lnquery_inc)
#     logging.info(f'{trn_ln_object_name}:',trn_lnquery_inc)
#     trn_lnsf_cursor.execute(trn_lnquery_inc)
#     transactionslastdatemodified=trn_lnsf_cursor.fetchone()[0]
#     logging.info(f'{trn_ln_object_name}:',transactionslastdatemodified)
#     return transactionslastdatemodified


# trn_lnlastdatemodifieddate='2021-09-21 00:00:00'#trn_lnlast_date_modified()
# etrn_lnlastdatemodifieddate='2021-09-26 00:00:00'
trn_lnlastdatemodifieddate=last_date_modified(trn_ln_object_name) 
logging.info(f'{trn_ln_object_name}:',trn_lnlastdatemodifieddate)


trn_lnnsquery=trn_lnnsquery + f" where {trn_ln_lastdatemodified} >='{trn_lnlastdatemodifieddate}' order by {trn_lnorder_by}"


def trn_ln_extract_records():
    
    trn_lnsf_conn=get_sf_connect()
    trn_lnsf_cursor=trn_lnsf_conn.cursor()
    """
        Generating ETL Process ID 
    """
    try:
        etl_process_audit(PLATFORM=trn_lnplatform,WORKFLOWNAME=trn_ln_object_name,STATUS='Running')
        logging.info(f'{trn_ln_object_name}:ETL_process id is generated')
       
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:ETL_process id is generation failed')
       
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" ETL_process id is generation failed ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    try:
        trn_lnprocess_id=etl_process_id(object_name=trn_ln_object_name)
        logging.info(f'{trn_ln_object_name}:',"etl_process_id",trn_lnprocess_id)
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:Failed to fetch etl_process_id')
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Failed to fetch etl_process_id ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    
    """
        Taking Source count
    """
    try:
        trn_lnsourcecount=source_count(object_name=trn_ln_object_name,lastmodifieddate=trn_lnlastdatemodifieddate)
        logging.info(f'{trn_ln_object_name}:',f'source_count for {trn_ln_object_name} is :',trn_lnsourcecount)
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:Failed at fetch source count')
        
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Failed at fecth source count ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    """
        Inserting Audit table 
    """
    try:
        insert_to_audit(task=extraction_task,object=trn_ln_object_name,source_type='NetSuite_DataBase',target_type="S3",package=trn_lnplatform,status='RUNNING',PROCESS_ID=trn_lnprocess_id,sourcecount=trn_lnsourcecount)
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:Failed to insert etl_audit_table')
     
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Failed to insert etl_audit_table ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    i=1
    try:
        trn_lnns_conn  =con()
        trn_lncursor=trn_lnns_conn.cursor()
        trn_lnfile_name=f'{trn_ln_object_name}_{trn_lnfile_date}.{trn_lnformat}'
        trn_lnpath=f'{trn_lnaws_path}{trn_ln_object_name}/{trn_lnfile_name}'
        # path=f'E:/{object_name}/{file_name}'
        try:
            trn_lnresult=fetching_data(trn_lncursor,trn_lnnsquery)
        except Exception as e:
            logging.info(f'{trn_ln_object_name}:',e)
            logging.info(f'{trn_ln_object_name}:Failed at fetching data from source')
            # logging.info('Fialed at fetchuing data')
            trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Fialed at fetchuing data from source ",e=e)
            send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
            return
        try:
            csv_genarator(trn_lnresult,trn_lncolumn,trn_lnpath)  

        except Exception as e:
            logging.info(f'{trn_ln_object_name}:Failed at file Generation and uploading to S3 ')    
           
            trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Failed at file Generation and uploading to S3 ",e=e)
            send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
            return
        trn_lndf=pd.DataFrame(trn_lnresult,columns=trn_lncolumn )
        trn_lncount=len(trn_lndf)
        
        try:
            insert_file_log(PROCESS_ID=trn_lnprocess_id,package=trn_lnplatform,
                            object=trn_ln_object_name,FILE_NAME=trn_lnfile_name,
                            file_count=trn_lncount,status="COMPLETED")
        except Exception as e:
            logging.info(f'{trn_ln_object_name}:',e)
            logging.info(f'{trn_ln_object_name}:Failed to insert file deatils in ETL FILE LOG ')
            
            trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
            send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)

        logging.info(f'{trn_ln_object_name}:',trn_lnfile_name,trn_lncount)
        trn_lnremark=''
        
        try:
            update_to_audit(rows_insert=0,remark=trn_lnremark,
                            enddate=trn_lnend_date,process_id=trn_lnprocess_id)
            logging.info(f'{trn_ln_object_name}:ETL Audit is updated sucessfully ')
           
        except Exception as e:
            logging.info(f'{trn_ln_object_name}:',e)
            logging.info(f'{trn_ln_object_name}:',"Failed to update ETL Audit table ")
         
            trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=extraction_task,transaction_date=trn_lnend_date,job=" Failed to update ETL Audit table  ",e=e)
            send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:Failed at Extraction of data from source')
 
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=trn_lntask,transaction_date=trn_lnend_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(trn_lncontent=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
def trn_ln_s3_sf():
    try:
        trn_ln_process_id=etl_process_id(object_name=trn_ln_object_name)
        logging.info(f'{trn_ln_object_name}:',"etl_process_id",trn_ln_process_id)
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:Failed to fetch etl_process_id')       
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=trn_lntask,transaction_date=trn_lnend_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    
    try:
        insert_to_audit(task=trn_lntask,object=trn_ln_object_name,source_type=stage,target_type=target,package=trn_lnplatform,status='RUNNING',PROCESS_ID=trn_ln_process_id,sourcecount=0)
        logging.info(f'{trn_ln_object_name}:',"ETL_AUDIT_TABLE is inserted")
        
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:',"Failed to insert ETL_audit_table ")
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=trn_lntask,transaction_date=trn_lnend_date,job="  Failed to insert ETL_audit_table  ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    trn_ln_copy_query=trn_ln_copy_command.format(filename=f'{trn_ln_object_name}_{trn_lnfile_date}.csv',process_id=trn_ln_process_id)
    logging.info(trn_ln_copy_query)
    try:
        copy_command(trn_ln_copy_query) 
        logging.info(f'{trn_ln_object_name}:',f'completd insertion of file to Snowflake database')
        
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        trn_lnremark=f'{trn_ln_object_name}_{trn_lnfile_date}'
        logging.info(f'{trn_ln_object_name}:Failed at inserting data into Landing schema/Table')
        error_to_audit(object=trn_ln_object_name,task=trn_lntask,remark=trn_lnremark)
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=trn_lntask,transaction_date=trn_lnend_date,job=f"  Failed at inserting data into Landing schema/Table for {trn_lnremark}  ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return
    try:
        update_to_audit(rows_insert=1,remark='',
                        enddate=trn_lnend_date,process_id=trn_ln_process_id)
        logging.info(f'{trn_ln_object_name}:ETL_audit_table is updated')
    except Exception as e:
        logging.info(f'{trn_ln_object_name}:',e)
        logging.info(f'{trn_ln_object_name}:Failed to update ETL_audit_table')
        trn_ln_ns_failed_constant=trn_lnns_failed_content.format(task=trn_lntask,transaction_date=trn_lnend_date,job=f"  Failed to update ETL_audit_table  ",e=e)
        send_email(content=trn_ln_ns_failed_constant ,Subject=trn_lnns_failed_subject)
        return


