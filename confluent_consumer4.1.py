# ---------------------------- Forecasting Algorithm ---------------------------- #
# The following program consumes messages from a specific topic and partition     #
# and applies the implemented forecasting algorithm to keep the top(k) (best k)   #
# out of all the different simulations that it's handling.                        #
# ------------------------------------------------------------------------------- #

from confluent_kafka import Consumer, KafkaError, TopicPartition
import argparse                         # -- for the arguments in running -- #
import signal                           # -- for handling signals(e.g. ctrl+C) -- #
import sys                              # -- access to functions that interact with the interpreter -- #
import os                               # -- to kill simulations -- #
from pymongo import MongoClient         # -- to send info to Mongodb -- #
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
print('The partition from which we are consuming: ', partition)

# Consumer definition
c = Consumer({
    'bootstrap.servers': '192.168.56.101:9092',
    'group.id': 'group_cells',
    'auto.offset.reset': 'earliest'
})

# The specific topic & partition from which we consume/pull messages
#c.subscribe(['cells'])
c.assign([TopicPartition('test', partition)])

# The desirable amount of best simulations
k = 5;
# Initialize index to Top_k, range(0,k), k: maximum value
index = 0;
# Define & Initialize Top_k list
top_k = [0 for i in range(k)]

d = 0;
deleted = []
# Variable that counts the number of consumed messages
message_counter = 0;

# Create pandas DataFrames
# A DataFrame that containts info about Time and alive cells of each running sim
# Time[0,1440,1] is the index, each PID is a different column containing the alive cells
data = list(range(0,1441,1))
simulations = pd.DataFrame(columns=['Time'], data=data)
simulations.set_index('Time', inplace=True)
# A DataFrame than contains all PIDs with info about downtrend
nominated = pd.DataFrame(columns=['PID', 'DownTrend', 'CountDown'])

# Create a mongo client object to the running mongod instance
mongo_client = MongoClient()
# MongoDB will create the db if it does not exist, and make a connection to it
#mongo_db = mongo_client["db_{}".format(pid)]
mongo_db = mongo_client["db_random"]
# Create a collection named cells
mongo_col = mongo_db.cells

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

    # Message handling
    # Decode received message
    message = '{}'.format(msg.value().decode('utf-8'));
    #print(message)
    # +1 messages consumed (increase the counter)
    message_counter += 1;
    # Split the message into a list using as separator ';'
    msg_list = message.split(";")
    print(msg_list)
    # Assign values to specifically INTEGER named variables
    pid = int(msg_list[0])
    if(pid in deleted):
        continue
    #pwdx = '{}'.format(check_output(['pwdx', str(pid)]).decode('utf-8'))
    #print(pwdx)
    #path = pwdx[(pwdx.find(":") + 2):]
    path = msg_list[5]
    #print(path)
    time = int(round(float(msg_list[1])))
    #time = round(float(msg_list[1]),2)
    #if(time!=1440.02):
    #    time = int(round(float(msg_list[1])))
    print('Current pid: ', pid, '|| current time: ', time)
    # The values are normalized
    alive = float(msg_list[2])
    apoptotic = float(msg_list[3])
    necrotic = float(msg_list[4])

    # Send only the Time=1440.02
    #if(time==1440):
        # ignore the message. Continue consuming
    #    continue

    #time = int(round(float(msg_list[1])))
    print('Current pid: ', pid, '|| current time: ', time)
    # Store message to MongoDB
    # Create a dictionary with the data
    msg_dict = {"PID": pid, "Path": path, "Time": time, "Alive": alive, "Apoptotic": apoptotic, "Necrotic": necrotic}
    #print(msg_dict)
    # Insert data to mongo_col
    doc = mongo_col.insert_one(msg_dict)

    # Firstly, all agents are alive.
    # time=0 means that this is the first message of this specific simulation that comes
    if(time==0):
        # Set normalization value
        norm_value = alive;
        # Normalize alive
        alive = alive/norm_value;
        # Creat a new column named with sim's pid
        new_PIDcolumn = []
        # Fill the new column with NaN
        for i in range(0,1441,1):
            # Assign the value 'alive' at the 1st position (time=0)
            if(i==0):
                new_PIDcolumn.append(alive)
                continue
            new_PIDcolumn.append(np.nan)
        # Push new PID column in the 'simulations' DataFrame. 
        simulations[pid] = new_PIDcolumn
        print(simulations)
        # Add the simulation row to the nominated DataFrame
        nominated = nominated.append({'PID': pid, 'DownTrend': 0, 'CountDown': 0}, ignore_index=True)
        print(nominated)
        #print(nominated['CountDown'].where(nominated['PID'] == pid).last_valid_index())
        # Create Top_k list with the 1st 5 new different PID coming simulations
        if(index < k):
            # Add new sim to the top_k list
            top_k[index] = pid
            print('Top_k list: ', top_k)
        # Increase index = shows the number of until now different running simulations
        index += 1;
        print('index: ', index)
        # continue consuming
        continue

    # For every other timepoint > 0 
    # Last valid time index for current simulation
    pr_time = simulations[pid].last_valid_index()
    print("Last valid time index: ", pr_time)
    limit = int(time - pr_time - 1)
    print('Limit: ', limit)
    # Append alive's value to the respective position
    alive = alive/norm_value;
    simulations.at[time,pid] = alive    # e.g. simulations.at[30,x]=1.02
    # linear interpolation to all the intermediate values
    # interpolate values between value 'time' and 'pr_time'
    simulations[pid] = simulations[pid].interpolate(method='slinear',limit_area='inside',limit=limit)
    print(simulations)

    # If top_k is full(index>=k) and a new simulation comes, chech if it should be added to the top_k list
    if(index > k):
        ind = nominated['CountDown'].where(nominated['PID'] == pid).last_valid_index()
        print('nominated index: ', ind)
        # Find the worst of all top_k's (top_k[0]: worst of the best)
        # Every time top_k updated according to the maximum time value of each of the top_k sims
        minimum = 1440;
        for sim in top_k:
            if(minimum > simulations[sim].last_valid_index()):
                # Set as minimum the last timepoint for which this sim has a valid value.
                minimum = simulations[sim].last_valid_index()
            print('Last valid index: ', simulations[sim].last_valid_index(), 'for simulation: ', sim)
        print('Minimum time: ', minimum)
        # minimum = minimum timepoint
        # Then, for this specific minimum timepoint, find the worst of the top_k
        i=0
        maximum=0
        # The 1st of the top_k is the worst
        #worst = top_k[0]
        for sim in top_k:
            print('time: ', minimum, 'simulation: ', sim)
            if(maximum < simulations.at[minimum,sim]):
                # maximum alive value
                maximum = simulations.at[minimum,sim]
                # pid of the worst simulation of the top_k
                tmp = top_k[0]
                top_k[0] = sim
                top_k[i] = tmp
            i += 1
        print('Top_k list: ', top_k)
        print('Worst of Top_k: ', top_k[0])

        # If timestamp > 0 & the current pid is not included in the top_k list
        if(minimum > 0 and (pid not in top_k)):
            # Check if for timepoint=time, threshold has generated value 
            if not(math.isnan(simulations.at[time,top_k[0]])):
                # Check if current alive is less than the Top_k's maximum - threshold
                print('Current pid: ', pid, 'and threshold: ', top_k[0])
                print('Current alive: ', alive, 'and threshold alive: ', simulations.at[time,top_k[0]])
                # if this specific time, the aliveNO of current's sim is less than the threshold's one
                if(alive < simulations.at[time,top_k[0]]):
                    # Throw the previous threshold out of the top_k list & insert the current sim in the top_k list
                    top_k[0] = pid
                # If the new one is not better than the worst one
                else:
                    # Check if continues to have steady decrease
                    # If previous measurement shows that the downtrend is increasing
                    print('nominated.at[ind,DownTrend]: ', nominated.at[ind, 'DownTrend'])
                    print('round(simulations.at[time, top_k[0]] - alive, 6): ', round(simulations.at[time, top_k[0]] - alive, 6))
                    if(nominated.at[ind, 'DownTrend'] < round(alive - simulations.at[time, top_k[0]], 6)):
                        # Update countDown
                        nominated.at[ind, 'CountDown'] += 1;
                    #else:
                    #    nominated.at[ind, 'CountDown'] = 0;
                    # Update last DownTrend
                    nominated.at[ind, 'DownTrend'] = round(alive - simulations.at[time, top_k[0]], 6);
            # if 'time' does not yet come in threshold means threshold is further back than the current one 
            else:
                # Check aliveNO at threshold's maximum time with the respective one of the current sim
                # Find the last time for which threshold has value
                maxTime = simulations[top_k[0]].last_valid_index()
                print('Maximum Time: ', maxTime)
                print('Current pid: ', pid, 'and threshold: ', top_k[0])
                print('Current alive at Maximum Time: ', simulations.at[maxTime, pid], 'and threshold alive at Maximum Time: ', simulations.at[maxTime,top_k[0]])
                # Same as before
                if(simulations.at[maxTime, pid] < simulations.at[maxTime, top_k[0]]):
                    # Throw the previous threshold out of the top_k list & insert the current sim in the top_k list
                    top_k[0] = pid
                # If the new one is not better than the worst one
                else:
                    print('nominated.at[ind, DownTrend]: ', nominated.at[ind, 'DownTrend'])
                    print('round(alive - simulations.at[maxTime, top_k[0]], 2): ', round(simulations.at[maxTime, pid] - simulations.at[maxTime, top_k[0]], 6))
                    # Check if continues to have steady decrease
                    if(nominated.at[ind, 'DownTrend'] < round(simulations.at[maxTime, pid] - simulations.at[maxTime, top_k[0]], 6)):
                        # Update countDown
                        nominated.at[ind, 'CountDown'] += 1;
                    #else:
                    #    nominated.at[ind, 'CountDown'] = 0;
                    # Update last DownTrend
                    nominated.at[ind, 'DownTrend'] = round(simulations.at[maxTime, pid] - simulations.at[maxTime, top_k[0]], 6);

            # I have the right to kill a simulation only if I handle more that k different sims
            # Kill simulation if its downtrend is increasing for 5 consecutive times
            if(nominated.at[ind, 'CountDown'] >= 5 and (pid not in top_k)):
                print("Simulation: ", pid, "killed in timepoint: ", time)
                print("Simulation: ", pid, "Path: ", path, "Timepoint: ",time, file=open("killedrandom.txt", "a"))
                #print("Path: ", file=open("output.txt", "a"))
                #print(path, file=open("output.txt", "a"))
                #print("Timepoint: ", file=open("output.txt", "a"))
                #print(time, file=open("output.txt", "a"))
                #file.close()
                # try:
                #     os.kill(pid,signal.SIGSTOP)
                # except OSError:
                #     print("The process with PID: ", pid, " is already dead")
                # else:
                #     print("Simulation: ", pid, " just killed")
                # Delete the info about this PID in MongoDB
                mongo_col.delete_many({'PID':pid})
                deleted.append(pid);
                d += 1;
                # Delete info about this pid in simulations DataFrame
                del simulations[pid]
                print(simulations)
                # drop a row by an index
                #nominated.drop(pid)
        print(nominated)
        # else do nothing

    # Signal handler (Ctrl + C)
    signal.signal(signal.SIGINT, signal_handler)

# Close consumer instance
c.close()