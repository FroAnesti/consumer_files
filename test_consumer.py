from confluent_kafka import Consumer, KafkaError, TopicPartition
import argparse                         # -- for the arguments in running -- #
import signal                           # -- for handling signals(e.g. ctrl+C) -- #
import sys                              # -- access to functions that interact with the interpreter -- #
import os                               # -- to kill simulations -- #
#from pymongo import MongoClient         # -- to send info to Mongodb -- #
import pandas as pd                     # -- to use pandas DataFrame -- #
import numpy as np                      # -- for computing tools -- #
import math                             # -- for math.isnan(x), x:number -- # 
from subprocess import check_output     # -- to return pid's working dir -- #

# If a signal generated (e.g. ctrl+C)
def signal_handler(sig, frame):
    print('Messages Consumed: ', message_counter, 'from partition: ', partition)
    # Exit from Python (defaulting to zero)
    sys.exit()


# Arguments
# Initiate the parser
parser = argparse.ArgumentParser()
# add long and short argument
parser.add_argument("-p", "--partition", required=True, help="set the partition")
# read arguments from the command line
args = parser.parse_args()
# Set partition variable as the given partition
partition = int(args.partition);
# print desirable partition
print('The partition from which we consume: ', partition)

# Consumer definition
c = Consumer({
    'bootstrap.servers': '192.168.56.101:9092',
    'group.id': 'group_cells',
    'auto.offset.reset': 'earliest'
})

# Topic in which the messages will be send
#c.subscribe(['cells'])
c.assign([TopicPartition('test', partition)])

message_counter = 0;

while True:
    # The poll API is designed to ensure consumer liveness.
    # As long as you continue to call poll, the consumer will stay in the group and 
    # continue to receive messages from the partitions it was assigned.
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    message = '{}'.format(msg.value().decode('utf-8'));
    print(message)
    message_counter += 1;
    #partition = msg.partition();
    #print("partition: %d" %partition)
    # Split the message into a list using as separator ';' where each word is a list item
    msg_list = message.split(";")
    #print(msg_list)
    # Assign values to specifically named variables
    pid = msg_list[0]
    timestamp = msg_list[1]
    alive = msg_list[2]
    apoptotic = msg_list[3]
    necrotic = msg_list[4]
    path = msg_list[5]
    #print(path)
    
    # Signal handler (Ctrl + C)
    signal.signal(signal.SIGINT, signal_handler)

# Close consumer instance
c.close()