import pymongo

class DataFederationClient:
    
    def __init__(self, **kwargs):
        self.client = pymongo.MongoClient(kwargs.get('uri'))
        self.db = self.client[kwargs.get('database')]
        self.coll = self.db[kwargs.get('collection')]

    
    def find(self, filter):
        '''
        Function to query data federation
        '''
        if not isinstance(filter, dict):
            print("Invalid filter, type must be dict")

        response = self.coll.find(filter)
        print(list(response))