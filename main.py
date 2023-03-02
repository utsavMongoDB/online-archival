import app.cluster_queries as cq
import schedule
import time
import datetime
import boto3
import config

# logging = log.setup_logger()

def run_archival():
    try:
        # Data federation URI
        uri = f"mongodb+srv://{config.user}:{config.password}@{config.server_addr}.mongodb.net/?retryWrites=true&w=majority"
        query_dict = {'st': "x+45200-066500"}

        # Create a client object to connect to the MongoDB database
        client = cq.MongoAtlasClient(uri, collection_name='sample_weatherdata', database='data', bucket_name='partner-demo')
        
        s3_resource = boto3.resource(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            aws_session_token=config.AWS_SESSION_TOKEN
        )
        
        print("fetching records for that match query")
        client.find_records_by_field(query_dict, s3_resource)

    except Exception as e:
        print(e)


if __name__ == "__main__":
    print("----------------started archival process----------------")
    run_archival()