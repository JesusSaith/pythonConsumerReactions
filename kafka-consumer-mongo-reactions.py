# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json
import subprocess

uri = "mongodb+srv://SAITH:MlHeSE6n1S7Sycsl@cluster0.h6hg7b4.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri)
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.memes
    print("MongoDB Connected successfully todo bien hasta aqui!")
except:
    print("Could not connect to MongoDB Aquiii")

consumer = KafkaConsumer('reaction',bootstrap_servers=['my-kafka-0.my-kafka-headless.jesussaith.svc.cluster.local:9092'])
# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    userid = record['userid']
    objectid = record['objectid']
    reactionid = record['reactionid']
    print("hola :)")


    # Create dictionary and ingest data into MongoDB
    try:
        memes_rec = {'userid': userid, 'objectid': objectid, 'reactionid': reactionid}
        print(memes_rec)
        memes_id = db.memes_reaction.insert_one(memes_rec)
        print("Data inserted with record ids", memes_id)

        subprocess.call(['sh', './test.sh'])
    except Exception as e:
        print("Could not insert into MongoDB")
        print(e)
    # Create bdnosql_sumary and insert groups into mongodb
    try:
        agg_result = db.memes_reaction.aggregate([
        {
            "$group": {
                "_id": {
                    'objectid': '$objectid',
                    'reactionid': '$reactionid'
                },
                "n": {"$sum": 1}
            }
        }
    ])
        db.memes_reaction_sumary.delete_many({})
        for i in agg_result:
            print(i)
            sumary_id = db.memes_reaction_sumary.insert_one(i)
            print("Sumary Reactions inserted with record ids: ", sumary_id)
            
    except Exception as e:
        print(f'group vy cought {type(e)}: ')
        print(e)
