def function_name( data_access ):

    # getting two MongoClient Object
    db_origin = data_access.get('mongo_more')
    db_target = data_access.get('mongo_bastidores')

    # Extract the data from 'db_origin'
    database = db_origin['db_sonae_mc']['Accidents']

    # Transform (aggregation, cleaning, etc)

    # Load (Storing the data in db_target)
    print([i for i in database.find()])