from pymongo import MongoClient
from bson.objectid import ObjectId


class AnimalShelter(object):
    """
    CRUD operations and advanced database analytics
    for the Animal collection in MongoDB
    """

    def __init__(self):
        """
        Initialize MongoDB connection (local instance)
        """
        try:
            self.client = MongoClient("mongodb://localhost:27017")
            self.database = self.client["AAC"]
            self.collection = self.database["animals"]
            self.create_indexes()
            print("Local MongoDB connection successful")
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    # ---------------- BASIC CRUD ---------------- #

    def create(self, data):
        """Insert a document into the collection"""
        try:
            if data and isinstance(data, dict):
                result = self.collection.insert_one(data)
                return bool(result.inserted_id)
            return False
        except Exception as e:
            print(f"Create error: {e}")
            return False

    def read(self, query=None):
        """Read documents matching a query"""
        try:
            if query is None:
                query = {}
            return list(self.collection.find(query))
        except Exception as e:
            print(f"Read error: {e}")
            return []

    # ---------------- INDEXING ---------------- #

    def create_indexes(self):
        """
        Create indexes on frequently queried fields
        to improve query performance
        """
        try:
            self.collection.create_index([("rescue_type", 1)])
            self.collection.create_index([("breed", 1)])
            self.collection.create_index([("age_upon_outcome_in_weeks", 1)])
            self.collection.create_index([
                ("rescue_type", 1),
                ("breed", 1)
            ])
            self.collection.create_index([("location", "2dsphere")])
            self.collection.create_index([("name", "text")])
        except Exception as e:
            print(f"Index creation error: {e}")

    # ---------------- OPTIMIZED READ ---------------- #

    def read_optimized(self, query, sort_field=None):
        """
        Optimized read using index hints
        """
        try:
            cursor = self.collection.find(query)

            if "rescue_type" in query and "breed" in query:
                cursor = cursor.hint([("rescue_type", 1), ("breed", 1)])
            elif "rescue_type" in query:
                cursor = cursor.hint([("rescue_type", 1)])
            elif "breed" in query:
                cursor = cursor.hint([("breed", 1)])

            if sort_field:
                cursor = cursor.sort(sort_field, 1)

            return list(cursor)
        except Exception as e:
            print(f"Optimized read error: {e}")
            return []

    # ---------------- AGGREGATION ANALYTICS ---------------- #

    def get_statistics_by_rescue_type(self, rescue_type):
        """
        Return top breeds and age statistics for a rescue type
        """
        pipeline = [
            {"$match": {"rescue_type": rescue_type}},
            {
                "$group": {
                    "_id": "$breed",
                    "count": {"$sum": 1},
                    "avgAge": {"$avg": "$age_upon_outcome_in_weeks"},
                    "minAge": {"$min": "$age_upon_outcome_in_weeks"},
                    "maxAge": {"$max": "$age_upon_outcome_in_weeks"}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        return list(self.collection.aggregate(pipeline))

    def get_breed_distribution(self):
        """
        Analyze breed usage across rescue types
        """
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "breed": "$breed",
                        "rescue_type": "$rescue_type"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.breed",
                    "rescue_types": {
                        "$push": {
                            "type": "$_id.rescue_type",
                            "count": "$count"
                        }
                    },
                    "total": {"$sum": "$count"}
                }
            },
            {"$sort": {"total": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def get_age_distribution(self, rescue_type=None):
        """
        Bucket dogs into age ranges (weeks)
        """
        match_stage = (
            {"$match": {"rescue_type": rescue_type}}
            if rescue_type else {"$match": {}}
        )

        pipeline = [
            match_stage,
            {
                "$bucket": {
                    "groupBy": "$age_upon_outcome_in_weeks",
                    "boundaries": [0, 52, 104, 156, 260, 520],
                    "default": "10+",
                    "output": {
                        "count": {"$sum": 1},
                        "avgAge": {"$avg": "$age_upon_outcome_in_weeks"}
                    }
                }
            }
        ]
        return list(self.collection.aggregate(pipeline))

    # ---------------- ADVANCED QUERIES ---------------- #

    def search_by_name(self, search_term):
        """Text search using MongoDB text index"""
        return list(
            self.collection.find(
                {"$text": {"$search": search_term}},
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})])
        )

    def explain_query(self, query):
        """
        Return query execution statistics
        """
        explanation = self.collection.find(query).explain()
        stats = explanation["executionStats"]

        return {
            "executionTimeMillis": stats["executionTimeMillis"],
            "documentsExamined": stats["totalDocsExamined"],
            "indexUsed": explanation["queryPlanner"]["winningPlan"]
        }
