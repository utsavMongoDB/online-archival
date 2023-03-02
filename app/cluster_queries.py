import pymongo
import json
from bson import json_util
import pandas as pd

class MongoAtlasClient:
    
    def __init__(self, uri, database, collection_name, bucket_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[database]
        self.coll = self.db[collection_name]
        self.bucket_name = bucket_name

    def find_records_by_field(self, query_dict, s3_resource):
        """
        Finds all documents in the collection that match a given query.

        Args:
            query_dict (dict): A dictionary representing the query to run.
        """
        try:
            documents_found = self.coll.find(query_dict)
            
            for i, document in enumerate(documents_found):
                json_data = json.loads(json_util.dumps(document))
                # Create parquet file from json
                df = pd.DataFrame(json_data, index=[0])
                parquet = df.to_parquet()
                # Upload to S3
                isUploaded = self.archive_record(parquet, s3_resource, document['st'])
                # Delete file from cluster
                if isUploaded:
                    self.delete_records_by_field(query_dict)
        except Exception as e:
            print(f"exception occured {e}")

    def archive_record(self, parquet, s3_resource, doc_id):
        success = False
        try:
            s3_resource.Bucket(self.bucket_name).put_object(Body=parquet, Key=f"archive/{doc_id}.parquet")
            s3_resource.Object(self.bucket_name, f"archive/{doc_id}.parquet").wait_until_exists()
            print("uploaded to s3")
            success = True
            
        except Exception as e:
            print(f"exception - {e}")
        
        finally:
            return success

    def delete_records_by_field(self, query_dict):
        try:
            print("deleting records from cluster")
            return self.coll.delete_many(query_dict)
        except Exception as e:
            print("Exception in delete records : ", e)
            return None