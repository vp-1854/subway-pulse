"""
Verify MongoDB and Cassandra are reachable.
  python infra/verify_connections.py
"""
from pymongo import MongoClient
from cassandra.cluster import Cluster

import os

client = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
print("MongoDB:", client.server_info()["version"])
client.close()

cluster = Cluster(os.environ.get("CASSANDRA_HOSTS", "localhost").split(","))
session = cluster.connect()
row = session.execute("SELECT release_version FROM system.local").one()
print("Cassandra:", row.release_version)
cluster.shutdown()
