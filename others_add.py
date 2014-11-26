from pymongo import MongoClient
from ast import literal_eval

client = MongoClient()
db = client.paper
clusters = db.clusters

nodes = literal_eval(open("D:\paper\missing_from_cluster.lst","r").read())

cluster_id = clusters.insert({'_id':"o","docs":nodes})

print cluster_id
