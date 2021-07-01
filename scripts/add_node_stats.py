import decimal
import os
import json
import psycopg2
from py2neo import Graph, Node, Relationship
from py2neo.bulk import create_relationships
from py2neo.bulk import merge_nodes
import time

#This  Script adds two stats : 'received_from_count' and 'sent_to_count'
#This allows to see with how many nodes the current node has interacted with.
#E.g We can have a block_count of 10.000 but only a sent_to_count of 1


get_node_stats = ( 
"Select source_account, count(*), blocktype from source_destination_stats   "
"group by source_account,blocktype                                          "
#"LIMIT 2" #for testing purpose only
)


    
with open("config.json") as json_data_file:
    config = json.load(json_data_file)
    
postgresql_config = config["postgresql"]["connection"]
conn = psycopg2.connect("host={} port={} dbname={} user={} password={}".format(postgresql_config["host"],postgresql_config["port"],postgresql_config["dbname"],postgresql_config["user"],postgresql_config["password"]))
conn.set_session(autocommit=False)
postgresql_cursor = conn.cursor("sel_all_relations") 
postgresql_cursor.itersize = 50000


t0 = time.time()
print("Add stats 'received_from_count' and 'sent_to_count' to all nodes.")

postgresql_cursor.execute(get_node_stats) 
#rows = postgresql_cursor.fetchall()
print("Exec SQL finished for {} node stats".format(postgresql_cursor.rowcount))

mem_nodes_sent = []
node_keys_sent = ["address", "sent_to_count"]
mem_nodes_received = []
node_keys_received = ["address", "received_from_count"]

count = 0
t0 = time.time()
t1 = time.time()

neo4j_config = config["neo4j"]["connection"]
g = Graph("bolt://{}:{}".format(neo4j_config["host"],neo4j_config["port"]), auth=(neo4j_config["user"], neo4j_config["password"]))


for row in postgresql_cursor: 
    #print("{} {} {}".format(row[0], row[1], row[2]))
    if row[2] == 'SEND' :
        mem_nodes_sent.append([row[0], row[1]])
    if row[2] == 'RECEIVE' :
        mem_nodes_received.append([row[0], row[1]])  

    if count % 1000 == 0:
        print("count: {} node stats ".format(count),end="\r",)
    count += 1    
    if count % 50000 == 0:        
        #create nodes
        merge_nodes(g.auto(), mem_nodes_sent, ("Account", "address"), keys=node_keys_sent)
        merge_nodes(g.auto(), mem_nodes_received, ("Account", "address"), keys=node_keys_received)
        mem_nodes_sent = []
        mem_nodes_received = []        
        t1 = time.time()

#create nodes
merge_nodes(g.auto(), mem_nodes_sent, ("Account", "address"), keys=node_keys_sent)
merge_nodes(g.auto(), mem_nodes_received, ("Account", "address"), keys=node_keys_received)
           
     
print("Exported Everything in {} seconds".format(t1-t0))
