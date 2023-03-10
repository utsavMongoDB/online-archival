import app.cluster_queries as cluster_queries
import schedule
import time
import datetime
import boto3
import config
import app.federation_queries as federation

def run_archival():
    try:
        # MongoDB Cluster URI
        uri = f"mongodb+srv://{config.USER}:{config.PASSWORD}@{config.SERVER_ADDR}.mongodb.net/"
        query_dict = {'delivered': True}


        # Create S3 client for S3
        s3_client = boto3.client(
            "s3",
            region_name=config.AWS_REGION,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            aws_session_token=config.AWS_SESSION_TOKEN
        )

        # Create a client object to connect to the MongoDB database
        client = cluster_queries.MongoAtlasClient(uri=uri, database=config.DATABASE, collection=config.COLLECTION,
                                     bucket=config.S3_BUCKET, batch_size=config.BATCH_SIZE, s3_client=s3_client)

        print("fetching records")
        # client.update_many()
        client.find_and_archive(query_dict)

    except Exception as e:
        print(e)


def read_data():
    try:
        uri = f"mongodb+srv://{config.FED_USER}:{config.FED_PASSWORD}@{config.FED_SERVER_ADDR}.mongodb.net/?ssl=true&authSource=admin"

        query = {'st': "x+11600+070400"}
        federation_client = federation.DataFederationClient(uri=uri, database=config.FED_DATABASE, collection=config.FED_COLLECTION)

        federation_client.find(query)


    except Exception as e:
        print(e)

if __name__ == "__main__":
    print("----------------started archival process----------------")
    run_archival()