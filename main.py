import app.cluster_queries as cq
import app.archival_storage as arch
import schedule
import time
import datetime
import app.logger as log

logging = log.setup_logger()


AWS_ACCESS_KEY_ID="ASIA6IESFAOZQKPX7PPO"
AWS_SECRET_ACCESS_KEY="Bph1knQ1xhIESVYGm5lkzpQvnGPB2GIS33mmxKSn"
AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjEPP//////////wEaCXVzLWVhc3QtMSJIMEYCIQDCfV19frHhgtvlcK/eGG7Jlp3EQmQp7/GymwRPvtu/egIhAKMilyTY7gbTHoAKARMsO9Fqatq5Dt4aYlDJFIodnj91KsADCJz//////////wEQAxoMOTc5NTU5MDU2MzA3IgyxKvWgvhg6oe4HMFAqlAOG6WrV481EvydY48gMmIuvWQdA9LQKU7uravPaHxNtUsGphefEfd2PGIUTLMoFVdXW+Ou+19kVCf1vPVGoYIgsWk8NYlvgVsCL2krm6gvKh46Sx1K2T68T3EebOICq91ZUddZbEqENyTJn1VdIgSaot76v9+wa7rXef0ip1gyTOe9xfHCr3okJ9PxeHaTg1r6pRn/48gGYKiEIDxlKdGq2nPkZmasbf1viaHYTyYxAajrzMnIiD1boTqNeX6UT/IxAjOvRTEJeAD15w7Ld3XEuboKy+3ulDutlyektxU3MjEvOn3ffG7Hl7HcMeNRPQVXCFYnR3sE6KeIesGRod84sG2OykPabF0+U++YJLneEfu6OQWZfNPqU1jW0SNeHmDpZP1eK3zobmWyFp14tukkdINTyqge5CPgWcmddud+DNkaO8UImMl6hsIHyn4wK5u3Kr6/UoH47v8gB0SNXaoy7aMwFg4zqxX8EBzFug/1BmXxiEVJOG6mYhw7LzUtbMvJcDEwppwCnEIm1/SzhuZMfP1dsSzCOlYCgBjqlAYZa6w4iKdwK69yjkkVicaR+JlBZaMqpv4BxzEHoAN6mnLIfZUjklv9RIZ2ZJQndTQzUSKIiZz5OR8+f9shC/x2R6gbcyK3frTeziVctZcFA9E08auyvjGVkk1fH673X+WoBCsYd28dU/R04klj0mm0BpCVn1omV1BR3x33HpqTWxKFGcaFANxLm0m6SANR7Snt86Vj0VBFUVscq4BpXGt+DmzQYOA=="

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
                aws_access_key_id=AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN
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