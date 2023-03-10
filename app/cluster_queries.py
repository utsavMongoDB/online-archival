import pymongo
import json
from bson import json_util
import pandas as pd
from multiprocessing.pool import ThreadPool 
from bson.objectid import ObjectId

class MongoAtlasClient:
    
    def __init__(self, **kwargs):
        self.client = pymongo.MongoClient(kwargs.get('uri'))
        self.db = self.client[kwargs.get('database')]
        self.coll = self.db[kwargs.get('collection')]
        self.bucket = kwargs.get('bucket')
        self.batch_size = kwargs.get('batch_size')
        self.s3_client = kwargs.get('s3_client')
        self.archived_records = []

    def update_many(self):
        bulk_request = [ ]
        limit = 1000
        for doc in self.coll.find( {} ).limit( limit ):
            bulk_request.append(pymongo.UpdateOne( { '_id': doc['_id'] }, { '$set': { 'delivered': True } } ) )

        result = self.coll.bulk_write( bulk_request )
        print(result.matched_count, result.matched_count)
        print(f"Updated {limit}")


    def find_and_archive(self, query_dict):
        """
        Finds all documents in the collection that match a given query, convert to Parquet, push to S3 and 
        delete records from cluster.

        Args:
            query_dict (dict): A dictionary representing the query to run.
        """
        try:
            documents_found = self.coll.find(filter=query_dict)
            print(self.coll.count_documents(filter=query_dict))
            parquets = []

            print("starting conversion to parquet")
            for i, document in enumerate(documents_found):
                # Break at batch_size limit point 
                if i == self.batch_size:
                    break
                json_data = json.loads(json_util.dumps(document))
                # Create parquet file from json
                df = pd.json_normalize(json_data, max_level=0)
                parquets.append((df.to_parquet(), document['_id']))
            
            print(f"{len(parquets)} Parquets starting upload to S3")
            # Create threadpool for async archival process
            pool = ThreadPool(processes=len(parquets)*2) 
            pool.starmap(self.archive_record,  parquets)
            print(f"{len(self.archived_records)} archived")

            # Delete files from cluster in batch
            result = self.coll.bulk_write( self.archived_records )
            print(f"{result.deleted_count} documents deleted from hot data")

        except Exception as e:
            print(f"exception : {e}")


    def archive_record(self, parquet, doc_id):
        '''
        Helper method for uploading parquet file blob to S3
        '''
        try:
            # Upload to S3
            file_name = f"archive/{doc_id}.parquet"
            response = self.s3_client.put_object(Body=parquet, Bucket=self.bucket, Key=file_name)
            # Verify with status code if the file is uploaded successfully
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                # print(f"Document {doc_id} could not archived")
                raise
            # print(f"Document {doc_id}.parquet uploaded")
            self.archived_records.append(pymongo.DeleteOne( {"_id" : ObjectId(doc_id)} ) )

        except Exception as e:
            print(f"exception in archiving : {e}")