import yaml
from azure.storage.blob import BlockBlobService
from datetime import timedelta, datetime
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import time


def monitorAdf():
    try:      
        # Get subscription and service principle data from config file
        subscription_id =  configMap['connections']['subscription_id']
        rg_name = configMap['connections']['adf']['rg_name']
        df_name = configMap['connections']['adf']['df_name']
        df_pipeline_name=configMap['connections']['adf']['pipeline_name']
        ad_client_id=configMap['connections']['service_principal']['ad_clientid']
        ad_client_secret=configMap['connections']['service_principal']['ad_client_secret']
        ad_tenantid=configMap['connections']['service_principal']['ad_tenantid']
        #Make credential object
        credentials = ServicePrincipalCredentials(client_id=ad_client_id, secret=ad_client_secret, tenant=ad_tenantid)    
        adf_client = DataFactoryManagementClient(credentials, subscription_id)
        print('adf access success!')        
        # Create a pipeline run
        run_response = adf_client.pipelines.create_run(rg_name, df_name, df_pipeline_name, parameters={})
        # Monitor the pipeline run
        time.sleep(30)
        pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
        print("\n\tPipeline run status: {}".format(pipeline_run.status))
        filter_params = RunFilterParameters(
                                            last_updated_after=datetime.now() 
                                            - timedelta(1), 
                                            last_updated_before=datetime.now() + timedelta(1))
        query_response = adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id, filter_params)
        activity_output=query_response.value[0].output
        # Upload the activity output to Blob storage
        createBlob(activity_output)
    except Exception as e:
        print('Error occurred while accessing adf', e)

def createBlob(blobcontent):
    try:
        timenow = datetime.now()
        date_time = timenow.strftime("%m%d%Y-%H:%M:%S")
        st_blobcontent = str(blobcontent)
        blobStorageConnection = configMap['connections']['blobstorage']
        block_blob_service = BlockBlobService(account_name=blobStorageConnection['accountname'], account_key=blobStorageConnection['accountkey'])  
        block_blob_service.create_container(blobStorageConnection['containername'])
        block_blob_service.create_blob_from_text(blobStorageConnection['containername'],date_time+'.txt',st_blobcontent)
    except Exception as e:
        print('Error occurred while creating blob', e)
# Main method.
if __name__ == '__main__':
    configMap = {}
    with open('config.yml','r') as stream:
      configMap = yaml.load(stream)
    monitorAdf()


