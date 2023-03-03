import pymongo
import json
from bson import json_util
import bson
import pandas as pd
import pyarrow as pa

class MongoAtlasClient:
    
    def __init__(self, **kwargs):
        self.client = pymongo.MongoClient(kwargs.get('uri'))
        self.db = self.client[kwargs.get('database')]
        self.coll = self.db[kwargs.get('collection')]
        self.bucket = kwargs.get('bucket')

    def find_and_archive(self, query_dict, s3_client):
        """
        Finds all documents in the collection that match a given query, convert to Parquet, push to S3 and 
        delete records from cluster.

        Args:
            query_dict (dict): A dictionary representing the query to run.
            s3_client : S3 Client Object reference
        """
        try:
            documents_found = self.coll.find(filter=query_dict)

            for i, document in enumerate(documents_found):
                print("starting conversion")
                json_data = json.loads(json_util.dumps(document))
                # Create parquet file from json
                df = pd.json_normalize(json_data, max_level=0)
                parquet = df.to_parquet()
                # Upload to S3
                isUploaded = self.archive_record(parquet, s3_client, document['st'])
                # Delete file from cluster
                if isUploaded:
                    # self.delete_records_by_field(query_dict)
                    pass
        except Exception as e:
            print(f"exception occured {e}")


    def archive_record(self, parquet, s3_client, doc_id):
        '''
        Helper method for uploading parquet file blob to S3
        '''
        try:
            file_name = f"archive/{doc_id}.parquet"
            response = s3_client.put_object(Body=parquet, Bucket = self.bucket, Key=file_name)
            # Verify with status code if the file is uplaoded successfully
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("uploaded to s3")
                return True
            return False
            
        except Exception as e:
            print(f"exception - {e}")
            return False

    def delete_records_by_field(self, query_dict):
        '''
        Helper method for deleting records from MongoDB Cluster
        '''
        try:
            print("deleting records from cluster")
            return self.coll.delete_many(query_dict)
        except Exception as e:
            print("exception in delete records : ", e)
            return None