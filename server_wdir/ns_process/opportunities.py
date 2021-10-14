from types import prepare_class
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
from ns_utils import con,fetching_data,incremental_date,get_db_config,get_config,csv_genarator,\
                     stage_count,start_date,end_date,insert_to_audit,update_to_audit,error_to_audit,\
                     get_sf_connect,copy_command,etl_process_audit,etl_process_id,source_count,\
                         send_email,insert_file_log,stage_count,inc_filter_by,last_date_modified,truncate

"""
    Gobal variables 
"""

opportunities_object_name='Opportunities'
opportunities_format='csv'
opportunities_file_date=incremental_date()
opportunities_transaction_date=start_date()
opportunities_end_date = end_date()
opportunities_columns='columns'
opportunities_query='query'
ns=get_config('NS')
opportunities_ns_failed_subject=ns['constant']['ns_mail_failed']
opportunities_ns_failed_content=ns['constant']['ns_failed_content']
opportunities_ns_schema=ns['jdbc']['schema']
opportunities_local_path=ns['jdbc']['local_path']
opportunities_user=ns['sf']['user']
opportunities_account=ns['sf']['account']
opportunities_database=ns['sf']['database']
opportunities_password=ns['sf']['password']
opportunities_role=ns['sf']['role']
opportunities_landingschema=ns['sf']['landingschema']
opportunities_integartionschema=ns['sf']['integartionschema']
opportunities_warehouse=ns['sf']['warehouse']
opportunities_warehouse=ns['sf']['warehouse']
opportunities_schema_audit=ns['sf']['schema_audit']
opportunities_audit_table_name=ns['sf']['audit_table_name']
opportunities_etl_table_name=ns['sf']['etl_table_name']
opportunities_aws_path=ns['constant']['aws_path']
extraction_task=ns['constant']['extraction']
opportunities_task=ns['constant']['task']
stage=ns['constant']['stage']
target=ns['constant']['target']

db_columns=get_db_config(objectname=opportunities_object_name,source=opportunities_columns)
opportunities_column_p1=db_columns['phase_1']
opportunities_column_p2=db_columns['phase_2']
opportunities_column_p3=db_columns['phase_3']
opportunities_db_query=get_db_config(objectname=opportunities_object_name,source=opportunities_query)
opportunities_nsquery_P1=opportunities_db_query['ns_phase_1']
opportunities_nsquery_P2=opportunities_db_query['ns_phase_2']
opportunities_nsquery_P3=opportunities_db_query['ns_phase_3']
opportunities_truncate_p1=opportunities_db_query['truncate_1']
opportunities_truncate_p2=opportunities_db_query['truncate_2']
opportunities_truncate_p3=opportunities_db_query['truncate_3']
opportunities_copy_p1=opportunities_db_query['copy_1']
opportunities_copy_p2=opportunities_db_query['copy_2']
opportunities_copy_p3=opportunities_db_query['copy_3']

opportunities_ns_query_P1=opportunities_nsquery_P1.format(table=opportunities_object_name)
opportunities_ns_query_P2=opportunities_nsquery_P2.format(table=opportunities_object_name)
opportunities_ns_query_P3=opportunities_nsquery_P3.format(table=opportunities_object_name)
opportunities_extract_date=opportunities_transaction_date
opportunities_platform=ns["jdbc"]["platform"]
opportunities_workflowname=opportunities_object_name


"""
    condition
"""
opportunities_filter_condition=inc_filter_by(opportunities_object_name)

opportunities_lastdatemodified=opportunities_filter_condition[0]
opportunities_order_by=opportunities_filter_condition[1]

opportunities_lastmodifieddate=last_date_modified(object_name=opportunities_object_name)
opportunities_lastmodifieddate_p1=opportunities_lastmodifieddate
opportunities_lastmodifieddate_p3=opportunities_lastmodifieddate
opportunities_lastmodifieddate_p2=opportunities_lastmodifieddate

opportunities_nsquery_P1=opportunities_ns_query_P1  + f" where {opportunities_lastdatemodified} >'{opportunities_lastmodifieddate_p1}' order by {opportunities_order_by}"
opportunities_nsquery_P2=opportunities_ns_query_P2  + f" where {opportunities_lastdatemodified} >'{opportunities_lastmodifieddate_p2}' order by {opportunities_order_by}"
opportunities_nsquery_P3=opportunities_ns_query_P3  + f" where {opportunities_lastdatemodified} >'{opportunities_lastmodifieddate_p3}' order by {opportunities_order_by}"



def opportunities_extraction_phase_1():

    logging.info(f'{opportunities_object_name}:extraction of data is started for phase 1')
    try:
        opportunities_sf_conn=get_sf_connect()
        opportunities_sf_cursor=opportunities_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=opportunities_platform,WORKFLOWNAME=opportunities_object_name,STATUS='Completed')
            logging.info(f'{opportunities_object_name}:','ETL_process id is generated')
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e) 
            opportunities_remark=str(e)+' at etl Process id'
            logging.info(f'{opportunities_object_name}:ETL_process id is generation failed')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return

        try:
            opportunities_process_id=etl_process_id(object_name=opportunities_object_name)
            logging.info(f'{opportunities_object_name}:',"etl_process_id",opportunities_process_id)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e)+ ' at fetching etl process id '
            logging.info(f'{opportunities_object_name}:Failed to fetch etl_process_id')
            error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            opportunities_sourcecount=source_count(object_name=opportunities_object_name,lastmodifieddate=opportunities_lastmodifieddate)
            logging.info(f'{opportunities_object_name}:',f'source_count for {opportunities_object_name} is :',opportunities_sourcecount)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark= str(e) + ' at soure _count'
            logging.info(f'{opportunities_object_name}:Failed at fecth source count')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=opportunities_object_name,source_type=f'{opportunities_platform}_DataBase',target_type="S3",package=opportunities_platform,status='RUNNING',PROCESS_ID=opportunities_process_id,sourcecount=opportunities_sourcecount)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e) + " at inserting audit table"
            logging.info(f'{opportunities_object_name}:Failed to insert etl_audit_table')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        # checking source count is greater than 0
        opportunities_object_count=f"""select SOURCE_COUNT from "{opportunities_schema_audit}"."{opportunities_audit_table_name}" where PROCESS_ID={opportunities_process_id} and OBJECT='{opportunities_object_name}' and  PACKAGE='{opportunities_platform}' """
        logging.info(f'{opportunities_object_name}:',opportunities_object_count)
        opportunities_sf_cursor.execute(opportunities_object_count)
        opportunities_count_source=opportunities_sf_cursor.fetchone()[0]
        logging.info(f'{opportunities_object_name}:',opportunities_count_source)
        opportunities_remark=""
        if opportunities_sourcecount > 0:  
            opportunities_ns_conn=con()
            opportunities_cursor=opportunities_ns_conn.cursor()
            opportunities_file_name=f'{opportunities_object_name}_P1_{opportunities_file_date}.{opportunities_format}'
            opportunities_path=f'{opportunities_aws_path}{opportunities_object_name}/{opportunities_file_name}'
            try:
                opportunities_result1=fetching_data(cursor=opportunities_cursor,query=opportunities_nsquery_P1)    
            except Exception as e:
                    logging.info(f'{opportunities_object_name}:',e)
                    opportunities_remark= str(e) + f' at extraction data  for {opportunities_file_name}'
                    error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                    logging.info(f'{opportunities_object_name}:Failed at fetchuing data from source')
                    opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Fialed at fetchuing data from source ",e=e)
                    send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                    return
            try:
                csv_genarator(opportunities_result1,opportunities_column_p1,opportunities_path)  
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark= str(e) + f' At csv genearation   for {opportunities_file_name}'
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                logging.info(f'{opportunities_object_name}:Failed at file Generation and uploading to S3 ')    
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                return
            opportunities_df=pd.DataFrame(opportunities_result1,columns=opportunities_column_p1 )
            opportunities_total_records_count_P1=len(opportunities_df)
            logging.info(f'{opportunities_object_name}:',opportunities_total_records_count_P1)
            try:
                insert_file_log(PROCESS_ID=opportunities_process_id,package=opportunities_platform,
                                object=opportunities_object_name,FILE_NAME=opportunities_file_name,
                                file_count=opportunities_total_records_count_P1,
                                status="COMPLETED")
                opportunities_remark=''
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+' at inserting file log'
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                logging.info(f'{opportunities_object_name}:Failed to insert file deatils in ETL FILE LOG ')
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            try:
                update_to_audit(rows_insert=0,remark='',
                                enddate=opportunities_end_date,process_id=opportunities_process_id)

                logging.info(f'{opportunities_object_name}:updated etl audit table is completed')
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ " at Update audit table"
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        else:
            logging.info('source has zero count')
            opportunities_remark='source has zero count'
            try:
                update_to_audit(rows_insert=0,remark=opportunities_remark,
                                enddate=opportunities_end_date,process_id=opportunities_process_id)
                logging.info(f'{opportunities_object_name}:updated etl audit table is completed')
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ " at Update audit table"
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        
        
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        opportunities_remark=str(e) + " at extracTion data from source"
        error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
        logging.info(f'{opportunities_object_name}:Failed at Extraction of data from source')
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return

def opportunities_extraction_phase_2():
    
    logging.info(f'{opportunities_object_name}:extraction of data is started for phase 2')
    try:
        opportunities_sf_conn=get_sf_connect()
        opportunities_sf_cursor=opportunities_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=opportunities_platform,WORKFLOWNAME=opportunities_object_name,STATUS='Completed')
            logging.info(f'{opportunities_object_name}:','ETL_process id is generated')
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e) 
            opportunities_remark=str(e)+' at etl Process id'
            logging.info(f'{opportunities_object_name}:ETL_process id is generation failed')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return

        try:
            opportunities_process_id=etl_process_id(object_name=opportunities_object_name)
            logging.info(f'{opportunities_object_name}:',"etl_process_id",opportunities_process_id)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e)+ ' at fetching etl process id '
            logging.info(f'{opportunities_object_name}:Failed to fetch etl_process_id')
            error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            opportunities_sourcecount=source_count(object_name=opportunities_object_name,lastmodifieddate=opportunities_lastmodifieddate)
            logging.info(f'{opportunities_object_name}:',f'source_count for {opportunities_object_name} is :',opportunities_sourcecount)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark= str(e) + ' at soure _count'
            logging.info(f'{opportunities_object_name}:Failed at fecth source count')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=opportunities_object_name,source_type=f'{opportunities_platform}_DataBase',target_type="S3",package=opportunities_platform,status='RUNNING',PROCESS_ID=opportunities_process_id,sourcecount=opportunities_sourcecount)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e) + " at inserting audit table"
            logging.info(f'{opportunities_object_name}:Failed to insert etl_audit_table')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        # checking source count is greater than 0
        opportunities_object_count=f"""select SOURCE_COUNT from "{opportunities_schema_audit}"."{opportunities_audit_table_name}" where PROCESS_ID={opportunities_process_id} and OBJECT='{opportunities_object_name}' and  PACKAGE='{opportunities_platform}' """
        logging.info(f'{opportunities_object_name}:',opportunities_object_count)
        opportunities_sf_cursor.execute(opportunities_object_count)
        opportunities_count_source=opportunities_sf_cursor.fetchone()[0]
        logging.info(f'{opportunities_object_name}:',opportunities_count_source)
        opportunities_remark=""
        if opportunities_sourcecount > 0:  
            opportunities_ns_conn=con()
            opportunities_cursor=opportunities_ns_conn.cursor()
            opportunities_file_name=f'{opportunities_object_name}_P2_{opportunities_file_date}.{opportunities_format}'
            opportunities_path=f'{opportunities_aws_path}{opportunities_object_name}/{opportunities_file_name}'
            try:
                opportunities_result2=fetching_data(cursor=opportunities_cursor,query=opportunities_nsquery_P2)    
            except Exception as e:
                    logging.info(f'{opportunities_object_name}:',e)
                    opportunities_remark= str(e) + f' at extraction data  for {opportunities_file_name}'
                    error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                    logging.info(f'{opportunities_object_name}:Failed at fetchuing data from source')
                    opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Fialed at fetchuing data from source ",e=e)
                    send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                    return
            try:
                csv_genarator(opportunities_result2,opportunities_column_p2,opportunities_path)  
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark= str(e) + f' At csv genearation   for {opportunities_file_name}'
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                logging.info(f'{opportunities_object_name}:Failed at file Generation and uploading to S3 ')    
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                return
            opportunities_df=pd.DataFrame(opportunities_result2,columns=opportunities_column_p2 )
            opportunities_total_records_count_P2=len(opportunities_df)
            logging.info(f'{opportunities_object_name}:',opportunities_total_records_count_P2)
            try:
                insert_file_log(PROCESS_ID=opportunities_process_id,package=opportunities_platform,
                                object=opportunities_object_name,FILE_NAME=opportunities_file_name,
                                file_count=opportunities_total_records_count_P2,
                                status="COMPLETED")
                opportunities_remark=''
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+' at inserting file log'
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                logging.info(f'{opportunities_object_name}:Failed to insert file deatils in ETL FILE LOG ')
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            try:
                update_to_audit(rows_insert=0,remark='',enddate=opportunities_end_date,
                                    process_id=opportunities_process_id)
                logging.info(f'{opportunities_object_name}:updated etl audit table is completed')
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ " at Update audit table"
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        else:
            logging.info('source has zero count')
            opportunities_remark='source has zero count'
            try:
                update_to_audit(rows_insert=0,remark=opportunities_remark,
                                enddate=opportunities_end_date,process_id=opportunities_process_id)
                logging.info(f'{opportunities_object_name}:updated etl audit table is completed')
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ " at Update audit table"
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        
        
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        opportunities_remark=str(e) + " at extracion data from source"
        error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
        logging.info(f'{opportunities_object_name}:Failed at Extraction of data from source')
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return



def opportunities_extraction_phase_3():
    logging.info(f'{opportunities_object_name}:extraction of data is started for phase 3')
    try:
        opportunities_sf_conn=get_sf_connect()
        opportunities_sf_cursor=opportunities_sf_conn.cursor()
        """
            Generating ETL Process ID 
        """
        try:
            etl_process_audit(PLATFORM=opportunities_platform,WORKFLOWNAME=opportunities_object_name,STATUS='Completed')
            logging.info(f'{opportunities_object_name}:','ETL_process id is generated')
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e) 
            opportunities_remark=str(e)+' at etl Process id'
            logging.info(f'{opportunities_object_name}:ETL_process id is generation failed')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" ETL_process id is generation failed ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return

        try:
            opportunities_process_id=etl_process_id(object_name=opportunities_object_name)
            logging.info(f'{opportunities_object_name}:',"etl_process_id",opportunities_process_id)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e)+ ' at fetching etl process id '
            logging.info(f'{opportunities_object_name}:Failed to fetch etl_process_id')
            error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to fetch etl_process_id ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        
        """
                Taking Source count
        """
        try:
            opportunities_sourcecount=source_count(object_name=opportunities_object_name,lastmodifieddate=opportunities_lastmodifieddate)
            logging.info(f'{opportunities_object_name}:',f'source_count for {opportunities_object_name} is :',opportunities_sourcecount)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark= str(e) + ' at soure _count'
            logging.info(f'{opportunities_object_name}:Failed at fecth source count')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at fecth source count ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        """
            Inserting Audit table 
        """
        try:
            insert_to_audit(task=extraction_task,object=opportunities_object_name,source_type=f'{opportunities_platform}_DataBase',target_type="S3",package=opportunities_platform,status='RUNNING',PROCESS_ID=opportunities_process_id,sourcecount=opportunities_sourcecount)
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e) + " at inserting audit table"
            logging.info(f'{opportunities_object_name}:Failed to insert etl_audit_table')
            # logging.info('Failed to insert etl_audit_table')
            error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to insert etl_audit_table ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
        opportunities_object_count=f"""select SOURCE_COUNT from "{opportunities_schema_audit}"."{opportunities_audit_table_name}" where PROCESS_ID={opportunities_process_id} and OBJECT='{opportunities_object_name}' and  PACKAGE='{opportunities_platform}' """
        logging.info(f'{opportunities_object_name}:',opportunities_object_count)
        opportunities_sf_cursor.execute(opportunities_object_count)
        opportunities_count_source=opportunities_sf_cursor.fetchone()[0]
        logging.info(f'{opportunities_object_name}:',opportunities_count_source)
        opportunities_remark=""
        if opportunities_sourcecount > 0:
            opportunities_ns_conn  =con()
            opportunities_cursor=opportunities_ns_conn.cursor()
            opportunities_file_name=f'{opportunities_object_name}_P3_{opportunities_file_date}.{opportunities_format}'
            opportunities_path=f'{opportunities_aws_path}{opportunities_object_name}/{opportunities_file_name}'
            try:
                opportunities_result3=fetching_data(cursor=opportunities_cursor,query=opportunities_nsquery_P3)    
            except Exception as e:
                    logging.info(f'{opportunities_object_name}:',e)
                    opportunities_remark= str(e) + f' at extraction data  for {opportunities_file_name}'
                    error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                    logging.info(f'{opportunities_object_name}:Failed at fetchuing data from source')
                    opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Fialed at fetchuing data from source ",e=e)
                    send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                    return
            try:
                    csv_genarator(opportunities_result3,opportunities_column_p3,opportunities_path)  
            except Exception as e:
                    logging.info(f'{opportunities_object_name}:',e)
                    opportunities_remark= str(e) + f' At csv genearation   for {opportunities_file_name}'
                    error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                    logging.info(f'{opportunities_object_name}:Failed at file Generation and uploading to S3 ')    
                    logging.info('Failed at file Generation and uploading to S3 ')
                    opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at file Generation and uploading to S3 ",e=e)
                    send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                    return
            opportunities_df=pd.DataFrame(opportunities_result3,columns=opportunities_column_p3 )
            opportunities_total_records_count_P3=len(opportunities_df)
            logging.info(f'{opportunities_object_name}:',opportunities_total_records_count_P3)
            try:
                insert_file_log(PROCESS_ID=opportunities_process_id,package=opportunities_platform,
                                object=opportunities_object_name,FILE_NAME=opportunities_file_name,
                                file_count=opportunities_total_records_count_P3,status="COMPLETED")

            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+' at inserting file log'
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                logging.info(f'{opportunities_object_name}:Failed to insert file deatils in ETL FILE LOG ')
                logging.info('Failed to insert file deatils in ETL FILE LOG ')
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed to insert file deatils in ETL FILE LOG  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            try:
                update_to_audit(rows_insert=0,remark='',enddate=opportunities_end_date,
                                process_id=opportunities_process_id)
                logging.info(f'{opportunities_object_name}:updated etl audit table is completed')
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ " at Update audit table"
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        
        else:
            opportunities_remark='source has no records'
            logging.info(f'{opportunities_object_name}:source count is 0')

            try:
                update_to_audit(rows_insert=0,remark=opportunities_remark,
                                enddate=opportunities_end_date,process_id=opportunities_process_id)
                logging.info(f'{opportunities_object_name}:updated etl audit table is completed')
            except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ " at Update audit table"
                error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
       
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        opportunities_remark=str(e) + " at extracion data from source"
        error_to_audit(object=opportunities_object_name,task=extraction_task,remark=opportunities_remark)
        logging.info(f'{opportunities_object_name}:Failed at Extraction of data from source')
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job="  Failed at Extraction of data from source  ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return

def opportunities_s3_sf():
    

    try:
        opportunities_process_id=etl_process_id(object_name=opportunities_object_name)
        logging.info(f'{opportunities_object_name}:',"etl_process_id",opportunities_process_id)
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        opportunities_remark=str(e)+ " at fetching etl processing id"
        error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
        logging.info(f'{opportunities_object_name}:Failed to fetch etl_process_id')
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=opportunities_task,transaction_date=opportunities_end_date,job="  Failed to fetch etl_process_id  ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return
    try:
        opportunities_conn=get_sf_connect()
        opportunities_sf_cursor=opportunities_conn.cursor()
        opportunities_object_count=f"""select SOURCE_COUNT from "{opportunities_schema_audit}"."{opportunities_audit_table_name}" where PROCESS_ID={opportunities_process_id} and OBJECT='{opportunities_object_name}' and  PACKAGE='{opportunities_platform}' """
        logging.info(f'{opportunities_object_name}:',opportunities_object_count)
        opportunities_sf_cursor.execute(opportunities_object_count)
        opportunities_count_source=opportunities_sf_cursor.fetchone()[0]
        logging.info(f'{opportunities_object_name}:',opportunities_count_source)
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        opportunities_remark= str(e) + ' at soure _count from audit table'
        logging.info(f'{opportunities_object_name}:Failed at fecth source count from audit table')
        error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=extraction_task,transaction_date=opportunities_end_date,job=" Failed at fecth source count ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return
    
    try:
        insert_to_audit(task=opportunities_task,object=opportunities_object_name,source_type=stage,target_type=target,package=opportunities_platform,status='RUNNING',PROCESS_ID=opportunities_process_id,sourcecount=0)
        logging.info(f'{opportunities_object_name}:',"ETL_AUDIT_TABLE is inserted")
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        opportunities_remark= str(e)+ 'at inserting audit table '
        logging.info(f'{opportunities_object_name}:',"Failed to insert ETL_audit_table ")
        error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
         
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=opportunities_task,transaction_date=opportunities_end_date,job="  Failed to insert ETL_audit_table  ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return
    try:
           truncate(query=opportunities_truncate_p1)
           truncate(query=opportunities_truncate_p2)
           truncate(query=opportunities_truncate_p3)
           logging.info('Truncate table is completed')
    except Exception as e:
        logging.info(f'{opportunities_object_name}:',e)
        remark=f'{opportunities_object_name}_{opportunities_file_date}'
        logging.info(f'{opportunities_object_name}:Failed at truncating table  from  Landing schema/Table')
        error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=remark)
        opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=opportunities_task,transaction_date=opportunities_end_date,job=f"  Falied at truncating Landing schema/Table for {remark}  ",e=e)
        send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
        return


    opportunities_j=0
    if opportunities_count_source>0:
        opportunities_j=1
        try:
         for opportunities_copy_query in(opportunities_copy_p1.format(filename=f'{opportunities_object_name}_P1_{opportunities_file_date}.csv',process_id=opportunities_process_id),opportunities_copy_p2.format(filename=f'{opportunities_object_name}_P2_{opportunities_file_date}.csv',process_id=opportunities_process_id),opportunities_copy_p3.format(filename=f'{opportunities_object_name}_P3_{opportunities_file_date}.csv',process_id=opportunities_process_id)) :            
                copy_command(opportunities_copy_query) 
                logging.info(f'{opportunities_object_name}:',f'completd insertion of file to Snowflake database for phase_{opportunities_j}')
                opportunities_j+=1 
        except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ f'{opportunities_object_name}_P{opportunities_j}_{opportunities_file_date}'
                logging.info(f'{opportunities_object_name}:Failed at inserting data into Landing schema/Table through copy command')
                error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=opportunities_task,transaction_date=opportunities_end_date,job=f"  Failed at inserting data into Landing schema/Table for {opportunities_remark}  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
                return
        try:
            update_to_audit(rows_insert=opportunities_j-1,remark='',
                            enddate=opportunities_end_date,process_id=opportunities_process_id)
            logging.info(f'{opportunities_object_name}:ETL_audit_table is updated')
        except Exception as e:
                logging.info(f'{opportunities_object_name}:',e)
                opportunities_remark=str(e)+ 'At update audit table '
                error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
                logging.info(f'{opportunities_object_name} Failed to update ETL_audit_table')
                opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=opportunities_task,transaction_date=opportunities_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
                send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)

    
    else:
        opportunities_remark='source has no records'
        logging.info(f'{opportunities_object_name}:source count is 0')
        
        try:
            update_to_audit(rows_insert=opportunities_j,remark=opportunities_remark,
                            enddate=opportunities_end_date,process_id=opportunities_process_id)
            logging.info(f'{opportunities_object_name}:ETL_audit_table is updated')
        except Exception as e:
            logging.info(f'{opportunities_object_name}:',e)
            opportunities_remark=str(e)+ 'At update audit table '
            error_to_audit(object=opportunities_object_name,task=opportunities_task,remark=opportunities_remark)
            logging.info(f'{opportunities_object_name} Failed to update ETL_audit_table')
            opportunities_ns_failed_constant=opportunities_ns_failed_content.format(task=opportunities_task,transaction_date=opportunities_end_date,job=f"  Failed to update ETL_audit_table  ",e=e)
            send_email(content=opportunities_ns_failed_constant ,Subject=opportunities_ns_failed_subject)
            return
