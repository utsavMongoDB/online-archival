import pymongo
import json
from bson import json_util
import app.logger as log

logging = log.setup_logger()

class MongoAtlasClient:
    
    def __init__(self, uri, database, collection_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[database]
        self.coll = self.db[collection_name]
        self.json_path = 'app/temp_data_store/json_data'


    def find_records_by_field(self, query_dict):
        """
        Finds all documents in the collection that match a given query.

        Args:
            query_dict (dict): A dictionary representing the query to run.

        Returns:
            int: The number of documents found.
        """
        documents_found = self.coll.find(query_dict)
        self.split_into_json(documents_found)
        return 1


    def split_into_json(self, cursor):
        """
        Split a MongoDB cursor into individual JSON files and write them to disk.

        Args:
            cursor (pymongo.cursor.Cursor): The cursor to split into individual
                documents.

        Returns:
            None
        """
        for i, document in enumerate(cursor):
            # Construct the file name using the document's _id field
            file_name = f"{self.json_path}/{document['st']}.json"

            # Write the document to a JSON file
            with open(file_name, 'w') as f:
                json.dump(json.loads(json_util.dumps(document)), f)

            logging.info(f"Exported document {i+1} to {file_name}")


    def delete_records_by_field(self, query_dict):
        try:
            return self.coll.delete_many(query_dict)
        except Exception as e:
            logging.exception("Exception in delete records : ", e)
            return None