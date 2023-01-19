from pyspark.sql.types import *
import datetime
import psycopg2
MnSt = 4
yr = 3
owmode = 'overwrite'
apmode = 'append'
class PostgresDbInfo:
    Host = "#PostgresDbInfo_Host"      
    Port = "#PostgresDbInfo_Port"
    PostgresDB = "#PostgresDbInfo_PostgresDB"  
    PostgresUrl = "jdbc:postgresql://" + Host + "/" + PostgresDB
    props = {"user":"#PostgresDbInfo_User", "password":"#PostgresDbInfo_Password", "driver": "org.postgresql.Driver"}   
    
class ConfiguratorDbInfo:
    Host = "#ConfiguratorDbInfo_Host"     
    Port = "#ConfiguratorDbInfo_Port"               
    PostgresDB = "Configurator" 
    Schema = "#ConfiguratorDbInfo_Schema"              
    PostgresUrl = "jdbc:postgresql://" + Host + "/" + PostgresDB
    props = {"user":"#ConfiguratorDbInfo_User", "password":"#ConfiguratorDbInfo_Password", "driver": "org.postgresql.Driver"}
class ConnectionInfo:
    JDBC_PARAM = "jdbc"
    SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    SQL_URL="jdbc:sqlserver://{0}:{1};databaseName={2};user={3};password={4}"
    

class Logger: 
    def __init__(self): 
        self._startTime = datetime.datetime.now()
        self._endTimeStr = None
        self._executionTime = None
        self._dateLog = self._startTime.strftime('%Y-%m-%d')
        self._startTimeStr = self._startTime.strftime('%H:%M:%S') 
        self._schema = StructType([
            StructField('Date',StringType(),True),
            StructField('Start_Time',StringType(),True),
            StructField('End_Time', StringType(),True),
            StructField('Run_Time',StringType(),True),
            StructField('File_Name',StringType(),True),
            StructField('DB',StringType(),True),
            StructField('EN', StringType(),True),
            StructField('Status',StringType(),True),
            StructField('Log_Status',StringType(),True),
            StructField('ErrorLineNo.',StringType(),True),
            StructField('Rows',IntegerType(),True),
            StructField('Columns',IntegerType(),True),
            StructField('Source',StringType(),True)
        ])

    def getSchema(self):
        return self._schema
    
    def endExecution(self):
        end_time = datetime.datetime.now()
        self._endTimeStr = end_time.strftime('%H:%M:%S')
        etime = str(end_time-self._startTime)
        self._executionTime = etime.split('.')[0]
    
    def getSuccessLoggedRecord(self, fileName, DBName, EntityName, rowsCount, columnsCount, source):
        return  [{'Date': self._dateLog,
                    'Start_Time': self._startTimeStr,
                    'End_Time': self._endTimeStr,
                    'Run_Time': self._executionTime,
                    'File_Name': fileName,
                    'DB': DBName,
                    'EN': EntityName,
                    'Status': 'Completed',
                    'Log_Status': 'Completed', 
                    'ErrorLineNo.': 'NA', 
                    'Rows': rowsCount, 
                    'Columns': columnsCount,
                    'Source': source
                }]

    def getErrorLoggedRecord(self, fileName, DBName, EntityName, exception, errorLineNo, source):
        return  [{'Date': self._dateLog,
                    'Start_Time': self._startTimeStr,
                    'End_Time': self._endTimeStr,
                    'Run_Time': self._executionTime,
                    'File_Name': fileName,
                    'DB': DBName,
                    'EN': EntityName,
                    'Status': 'Failed',
                    'Log_Status': str(exception),
                    'ErrorLineNo.': str(errorLineNo),
                    'Rows': 0,
                    'Columns': 0,
                    'Source': source
                }]
        
        
class PowerBISync:
    LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/token'
    LOGIN_REQUEST_PARAMS = {'grant_type': 'password',
         'username': '',
         'password': '',
         'client_id': '',
         'client_secret': '',
         'resource': 'https://analysis.windows.net/powerbi/api',
         'prompy': 'admin_consent'}
    WORKSPACE_ID = ''
    Masters_Dataset=""
    Sales_Dataset=""
    Purchase_Dataset=""
    Finance_Dataset=""
    Inventory_Dataset=""
    GET_WORKSPACE_DATASET = 'https://api.powerbi.com/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets'
    REFRESH_WORKSPACE_DATASETS = 'https://api.powerbi.com/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets/{0}/refreshes'
    REFRESH_NOTIFY_OPTION = {'notifyOption': 'MailOnFailure'}