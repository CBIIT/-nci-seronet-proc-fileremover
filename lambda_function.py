import boto3
import json
import logging
from seronetCopyFiles import fileCopy
from seronetdBUtilities import connectToDB
from seronetSnsMessagePublisher import sns_publisher


def lambda_handler(event, context):
    print('Loading function')
    # boto3 S3 initialization
    s3_client = boto3.client("s3")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ssm = boto3.client("ssm")
    
    
    try:
      host = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
      user = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
      dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
      password = ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
      destination_bucket_name = ssm.get_parameter(Name="file_destination_bucket", WithDecryption=True).get("Parameter").get("Value")
      JOB_TABLE_NAME = 'table_file_remover'
      accountId = boto3.client('sts').get_caller_identity().get('Account')
      # print('Account ID is: '+accountId)
      
      
    
      
      
      #read the message from the event
      message = event['Records'][0]['Sns']['Message']
      #convert the message to json style
      print(message)
      messageJson = json.loads(message)
       
      source_bucket_name = messageJson['bucketName']
      
      print('Source Bucket:'+source_bucket_name)
      message = event['Records'][0]['Sns']['Message']
      #convert the message to json style
      messageJson = json.loads(message)
      source_bucket_name = messageJson['bucketName']
        
      # determining which cbc bucket the file came from
      prefix = ''
      
      # Filename of object (with path) and Etag
      file_key_name = messageJson['key']
      
      print('Key file: '+ file_key_name)
                      
      if "guid" in messageJson and accountId == messageJson['accountId']:
          try:
            newestScanResults = len(messageJson['scanResults']) - 1
            if messageJson['scanResults'][newestScanResults]['result'] == 'Clean':
                # defining constants for CBCs
                bucket_name = ssm.get_parameter(Name = "bucket_name_list", WithDecryption=True).get("Parameter").get("Value")
                bucket_name_list = bucket_name.split(",")
                bucket_name_list = [s.strip() for s in bucket_name_list]
            
                prefix='UNMATCHED'
                for CBC in bucket_name_list:
                    if CBC in source_bucket_name:
                        prefix = CBC
    
            
                print('Prefix is: '+prefix)
                    # Copy Source Object
                if(prefix != 'UNMATCHED'):
                    try:
                        #connect to RDS
                        mydb = connectToDB(user, password, host, dbname)
                        #call the function to copy file
                        maxtry=3
                        result = fileCopy(s3_client, event, destination_bucket_name, maxtry, bucket_name_list)
                        
                        execution1 = f"SELECT COUNT(*) FROM {JOB_TABLE_NAME} WHERE file_md5 = %s"
                        mydbCursor=mydb.cursor(prepared=True)
                        file_md5=str(result['file_md5'])
                        
                        mydbCursor.execute(execution1,(file_md5,))
                        sqlresult = mydbCursor.fetchone()
                        if(sqlresult[0]>0 and result['file_status']=="COPY_SUCCESSFUL"):
                            result['file_status']="COPY_SUCCESSFUL_DUPLICATE"
                        elif(sqlresult[0]>0 and result['file_status']=="COPY_UNSUCCESSFUL"):
                            result['file_status']="COPY_UNSUCCESSFUL_DUPLICATE"
                        
                        
                        resultTuple = (result['file_name'], result['file_location'], result['file_added_on'], result['file_last_processed_on'], result['file_status'], result['file_origin'], result['file_type'], result['file_action'], result['file_submitted_by'], result['updated_by'], result['file_md5'])
                        #record the copy file result 
                        excution2 = "INSERT INTO "+ JOB_TABLE_NAME+" VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" 
                        mydbCursor.execute(excution2, resultTuple)
                        
                        
                        #publish message to sns topic
                        result['previous_function']="filecopy"
                        #add two more values to control whether or not send email or slack message
                        result['send_email']="yes"
                        result['send_slack']="yes"
                        
                        TopicArn_Success = ssm.get_parameter(Name="TopicArn_Success", WithDecryption=True).get("Parameter").get("Value")
                        TopicArn_Failure = ssm.get_parameter(Name="TopicArn_Failure", WithDecryption=True).get("Parameter").get("Value")
                        res=sns_publisher(result,TopicArn_Success,TopicArn_Failure)
                        print(res)
                        
                        
                        statusCode=200
                        message='File Processed'
                    except Exception as e:
                        raise e
                    finally:
                        #close the connection
                        mydb.commit()
                        mydb.close()
                    
                
                else:
                    statusCode=400
                    message='Desired CBC prefix not found'

          except Exception as error:
            statusCode=400
            #print('the message json is not correct')
            raise error
      else:
          statusCode=400
          print('The message json is incorrect')
    except Exception as err:
      raise err
        
    return {
       'statusCode': statusCode,
       'body': json.dumps(message)
    }
