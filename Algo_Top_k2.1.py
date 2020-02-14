# ----------------------------- Last module: Top_k ------------------------------ #
# Mongodb database='db_cells' includes all the info about all the completed       #
# simulations. From all the completed simulations we will keep only the best k,   #
# namely, the top k simulations.                                                  #
# We decide the top k out of all the locally top k simulations according to       #
# the alive cells' amount at the last timepoint, namely, time=1440.               #
# ------------------------------------------------------------------------------- #

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="darkgrid")
import pymongo
from pymongo import MongoClient
import argparse
import sys

# Optional arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", type=argparse.FileType('w'),
                    help="write report to FILE", metavar="FILE")				# -- Requires argument -- #
parser.add_argument("-p", "--plot",
					help="plot simulations' figures", action="store_true")		# --    No argument    -- #
args = parser.parse_args()

# Define k
k = 5;
top = [];
# Create DataFrame with the final Top_k, kx4
#df = pd.DataFrame(columns=['Time', 'Alive', 'Apoptotic', 'Necrotic'])
# Define query
query = {"Time": 1440}
# Create a mongo client object to the running mongod instance
mongo_client = MongoClient()
# Access the db where we send the info
mongo_db = mongo_client["db_random"]
# Create a collection which will include the cells dataframe
mongo_col = mongo_db.cells
# Return all documents in the "cells" where Time=1440, sorted by "PID"
# If we have 2 '1440' for each PID 
#doc = mongo_col.find({}, {"_id": 0 }).sort([("PID", pymongo.DESCENDING), ("Alive", pymongo.ASCENDING)]).limit(1)
#doc = mongo_col.aggregate({ "$elemMatch": { "Time": 30, "Alive":0.0 } } ).pretty()
# Return the document in the "cells" where Time=1440
# Sort the information stored in current db according to Alive value (ascending) at the last timepoint
doc = mongo_col.find(query,{"_id": 0 }).sort('Alive')

print('Top_k are the following')
minimum = 1137
run = 0
count = 0;
count_zeros = 0;
index = 0;
pid_count=0;
pid=0;
sims=0;
count_sims = 0;
last_alive = 0;
count_last = 0;

for dic in doc:
	#print(dic)
	print("AliveNO this: ", dic['Alive'])
	# Increase the number of simulations that locally survived
	count_sims += 1;
	# if we surpass the k and there is not an overlapping then...
	if (count_sims > k and dic['Alive'] != last_alive):
		# ...continue the procedure
		continue;

	# Update the number of global top simulations
	count_last += 1;
	# Add it to the top list
	top.append(dic['PID'])
	# Update the last came value of alive cells
	last_alive = dic['Alive']
	# Add it to the top list
	#print("AliveNO this: ", last_alive)

	# If a file is given as an option
	if args.file:
		print("Print info in file")
		filename = sys.argv[1][(sys.argv[1].find("=") + 1):]
		#print(filename)
		f = open(filename, "a")
		f.write(dic['Path'])
		f.write("\n")
		f.close()

print("All locally survived sims: ", count_sims)
print("Simulations that have the same number finallly (globally): ", count_last)

# If plot is given as an option
if args.plot:
	df_time_course = pd.DataFrame(columns=['Time','Alive','Apoptotic','Necrotic'])
	print("Create simulations' figures")

	for i in top:
		doc = mongo_col.find({'PID':i},{"_id": 0 }).sort("Time")
		for dic in doc:
			df_time_course = df_time_course.append({'Time': dic['Time'], 'Alive': dic['Alive'], 'Apoptotic': dic['Apoptotic'], 'Necrotic': dic['Necrotic']}, ignore_index=True)
		print(df_time_course)
		x = dic['Path'].find("run")
		run = dic['Path'][(x+3):dic['Path'].find("/output")]
		#print(run)

		fig, ax = plt.subplots(1, 1, figsize=(6,4), dpi=150)
		# plot Alive vs Time
		ax.plot(df_time_course.Time, df_time_course.Alive, 'g', label='alive')
		# plot Necrotic vs Time
		ax.plot(df_time_course.Time, df_time_course.Necrotic, 'k', label='necrotic')
		# plot Apoptotic vs Time
		ax.plot(df_time_course.Time, df_time_course.Apoptotic, 'r', label='apoptotic')
		# setting axes labels
		ax.set_xlabel('time (min)')
		ax.set_ylabel('N of cells')
		# Showing legend
		ax.legend()
		# Saving fig
		fig_fname =("./figures/random/run%s.png" %run)
		fig.tight_layout()
		fig.savefig(fig_fname)

		df_time_course = pd.DataFrame()


