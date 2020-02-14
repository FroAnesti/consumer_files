from pymongo import MongoClient

mongo_client = MongoClient()

#index = 10;

print(mongo_client.list_database_names())

#for db in mongo_client.list_database_names():
#	if(db!='db_pulse150_c2_c3'):
#		continue;
#	mongo_db = mongo_client[db]
#	mongo_col = mongo_db.cells
#	doc = mongo_col.find({'Time':1440},{ "_id": 0 }).sort("PID")
#	for dic in doc:
#		print(dic)
#	if(db=='admin' or db=='config' or db=='local'):
#		continue;
#	mongo_db = mongo_client[db]
#	mongo_col = mongo_db.cells
#	doc = mongo_col.find({},{ "_id": 0 }).sort("PID")
#	for dic in doc:
#		print(dic)


	#if (db!='db_cells1'):
	#	continue
	# if(db=='admin' or db=='config' or db=='local'):
	# 	continue;
 # 	#mongo_client.drop_database(db)
 # 	mongo_db = mongo_client[db]
 # 	mongo_col = mongo_db.cells
 # 	doc = mongo_col.find({},{ "_id": 0 }).sort("PID")
 # 	for dic in doc:
 # 		print(dic)
	#if("db_{}".format(index) == db):
# 	print(db)
# 	if("db_{}".format(index) == db):
#  		print('Exists')
#  		mongo_client.drop_database(db)
#  		index += 1;

#mongo_db = mongo_client["db_{}".format(index)]
# mongo_col = mongo_db.test_dict
# mydict = {'name': 'Harry', 'age' : 20}
# doc = mongo_col.insert(mydict)

#mongo_client.drop_database('db_random')