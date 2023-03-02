import app.cluster_queries as cq
import app.archival_storage as arch
import schedule
import time
import datetime
import app.logger as log
import config

logging = log.setup_logger()

def run_archival():
    try:
        # Data federation URI
        uri = "mongodb://admin:admin@weatherarchive-fmxyq.a.query.mongodb.net/?ssl=true&authSource=admin"
        query_dict = {'type' : "FM-13"}

        # Create a client object to connect to the MongoDB database
        client = cq.MongoAtlasClient(uri, 'sample', 'weather')
        logging.info("fetching records for that match query")
        client.find_records_by_field(query_dict)

        # Convert the retrieved records to the Parquet format
        logging.info("converting to parquet")
        archive_client = arch.ArchivalClient(
                aws_access_key_id=config.AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
                aws_session_token=config.AWS_SESSION_TOKEN
            )
        
        archive_client.convert_to_parquet()

        # Upload the Parquet file to an S3 bucket using AWS credentials
        archive_client.upload_to_S3()
        
        logging.info("deleting archived records from hot data")
        # client.delete_records_by_field(query_dict)

    except Exception as e:
        logging.exception(e)


if __name__ == "__main__":

    logging.info("----------------started archival process----------------")

    run_archival()
    # Schedule for every 1 minute
    # schedule.every(1).minutes.do(run_archival())
    
    # while True:
    #     # Checks whether a scheduled task
    #     # is pending to run or not
    #     schedule.run_pending()
    #     time.sleep(1)