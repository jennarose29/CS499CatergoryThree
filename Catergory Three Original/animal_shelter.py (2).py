from pymongo import MongoClient
from bson.objectid import ObjectId

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username="aacuser", password="SNHU1234"):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the AAC database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # Connection Variables
        #
        USER = username
        PASS = password
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 31580
        DB = 'AAC'
        COL = 'animals'
        
        #
        # Initialize Connection
        #
        try:
            self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
            self.database = self.client['%s' % (DB)]
            self.collection = self.database['%s' % (COL)]
            print("Connection successful")
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    def create(self, data):
        """
        Insert a document into the MongoDB collection
        
        Args:
            data (dict): Dictionary containing key/value pairs to insert
            
        Returns:
            bool: True if successful insert, False otherwise
        """
        try:
            if data is not None and isinstance(data, dict):
                # Insert the document into the collection
                result = self.collection.insert_one(data)
                # Check if insertion was successful by verifying inserted_id exists
                if result.inserted_id:
                    return True
                else:
                    return False
            else:
                raise Exception("Nothing to save, because data parameter is empty or not a dictionary")
        except Exception as e:
            print(f"An error occurred during create operation: {e}")
            return False

    def read(self, query=None):
        """
        Query for documents from the MongoDB collection
        
        Args:
            query (dict): Dictionary containing key/value pairs for the search criteria
                         If None, returns all documents
            
        Returns:
            list: List of documents if successful, empty list otherwise
        """
        try:
            # If no query provided, return all documents
            if query is None:
                query = {}
            
            # Ensure query is a dictionary
            if not isinstance(query, dict):
                print("Query parameter must be a dictionary")
                return []
            
            # Execute the find operation and convert cursor to list
            cursor = self.collection.find(query)
            result = list(cursor)
            
            return result
            
        except Exception as e:
            print(f"An error occurred during read operation: {e}")
            return []