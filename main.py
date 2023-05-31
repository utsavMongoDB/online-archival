import app.cluster_queries as cluster_queries
import schedule
import boto3
import config
import app.federation_queries as federation
from bson.objectid import ObjectId

def run_archival():
    try:
        print("----------------started archival process----------------")
        # MongoDB Cluster URI
        uri = f"mongodb+srv://{config.USER}:{config.PASSWORD}@{config.SERVER_ADDR}.mongodb.net/"
        query_dict = {'archive': True}

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
        uri = f"mongodb://{config.FED_USER}:{config.FED_PASSWORD}@{config.FED_SERVER_ADDR}.mongodb.net/test?ssl=true&authSource=admin"
        print("uri - ", uri)
        query = '<ADD_YOUR_QUERY>'
        federation_client = federation.DataFederationClient(
                uri=uri, 
                database=config.FED_DATABASE, 
                collection=config.FED_COLLECTION
            )
        federation_client.find(query)

    except Exception as e:
        print(e)

if __name__ == "__main__":
    run_archival()
    # schedule.every(config.ARCHIVE_FREQ).minutes.do(run_archival)

    # while True:
    #     # Checks whether a scheduled task is pending to run
    #     schedule.run_pending()
