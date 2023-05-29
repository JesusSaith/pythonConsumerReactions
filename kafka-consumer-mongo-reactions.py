# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json
import subprocess

uri = "mongodb+srv://beto:admin@cluster0.xtwgr69.mongodb.net/?retryWrites=true&w=majority"

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

    db = client.tkdapp
    print("MongoDB Connected successfully todo bien hasta aqui!")
except:
    print("Could not connect to MongoDB Aquiii")

consumer = KafkaConsumer('reaction',bootstrap_servers=['my-kafka-0.my-kafka-headless.btoarriola.svc.cluster.local:9092'])
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
        tkdapp_rec = {'userid': userid, 'objectid': objectid, 'reactionid': reactionid}
        print(tkdapp_rec)
        tkdapp_id = db.tkdapp_reactions.insert_one(tkdapp_rec)
        print("Data inserted with record ids", tkdapp_id)

        subprocess.call(['sh', './test.sh'])
    except Exception as e:
        print("Could not insert into MongoDB")
        print(e)
