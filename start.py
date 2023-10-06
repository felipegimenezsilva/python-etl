import Control, os
from pymongo import MongoClient
from dotenv import load_dotenv
from Transformation import exemple

# loading environment variables
load_dotenv()

# starting tel process
etl = Control.etl.ExtractTransformLoad()

# configuring databases
etl.databases.add('mongo_more',MongoClient(os.environ['uri_1']))
etl.databases.add('mongo_bastidores', MongoClient(os.environ['uri_2']))

# configuring ETL transformations
etl.add("title", exemple.function_name)
etl.add("titlex", None)

# final status
status = etl.run()

print(status)