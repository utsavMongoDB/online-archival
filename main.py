import app.cluster_queries as cq
import schedule
import time
import datetime
import boto3
import config


def run_archival(database, collection, s3_bucket):
    try:
        # Data federation URI
        uri = f"mongodb+srv://{config.user}:{config.password}@{config.server_addr}.mongodb.net/"
        query_dict = {'st': "x+47600-047900"}

        # Create a client object to connect to the MongoDB database
        client = cq.MongoAtlasClient(uri=uri, database=database, collection=collection, bucket=s3_bucket)

        s3_client = boto3.client(
            "s3",
            region_name=config.AWS_REGION,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            aws_session_token=config.AWS_SESSION_TOKEN
        )

        print("fetching records")
        client.find_and_archive(query_dict, s3_client)

    except Exception as e:
        print(e)


if __name__ == "__main__":
    print("----------------started archival process----------------")
    run_archival(database='sample_weatherdata',
                 collection='data', s3_bucket='partner-demo')
