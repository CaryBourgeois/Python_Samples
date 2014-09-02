#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import sys
import time
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import csv
from cassandra.cluster import Cluster
from cassandra.policies import DowngradingConsistencyRetryPolicy
from cassandra.policies import ConstantReconnectionPolicy
from cassandra.query import BatchStatement

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
handler.setLevel(logging.DEBUG)

mylogger = logging.getLogger("mylogger")
mylogger.addHandler(handler)
mylogger.setLevel(logging.DEBUG)

field_names = ['id', \
				'year', \
				'day_of_month', \
				'fl_date', \
				'airline_id', \
				'carrier', \
				'fl_num', \
				'origin_airport_id', \
				'origin', \
				'origin_city_name', \
				'origin_state_abr', \
				'dest', \
				'dest_city_name', \
				'dest_state_abr', \
				'dep_time',\
				'arr_time', \
				'actual_elapsed_time', \
				'air_time', \
				'distance']

cql_create_table = "CREATE TABLE flights ( \
	id int, \
	year int, \
	day_of_month int, \
	fl_date timestamp, \
	airline_id int, \
	carrier varchar, \
	fl_num int, \
	origin_airport_id int, \
	origin varchar, \
	origin_city_name varchar,\
	origin_state_abr varchar, \
	dest varchar, \
	dest_city_name varchar, \
	dest_state_abr varchar, \
	dep_time timestamp, \
	arr_time timestamp, \
	actual_elapsed_time int, \
	air_time int, \
	distance int, \
	air_time_grp int, \
	PRIMARY KEY (carrier, origin, air_time_grp, id));"
	
cql_insert_stmt = "INSERT INTO flights ( \
	id, \
	year, \
	day_of_month, \
	fl_date, \
	airline_id, \
	carrier, \
	fl_num, \
	origin_airport_id, \
	origin, \
	origin_city_name,\
	origin_state_abr, \
	dest, \
	dest_city_name, \
	dest_state_abr, \
	dep_time, \
	arr_time, \
	actual_elapsed_time, \
	air_time, \
	distance, \
	air_time_grp) \
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		
def main():
	#
	# connection variables
	#
	session = None
	stmt = None
	batch = None
	
	mylogger.info('Begin main()')
	
	#
	# make connection to cassandra
	#
	cluster = Cluster(['127.0.0.1'])
	metadata = cluster.metadata
	session = cluster.connect()
	mylogger.info('Connected to cluster: ' + metadata.cluster_name)
	for host in metadata.all_hosts():
		mylogger.info('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)
	
	
	#
	# execute CQL commands to build KEYSPACE and TABLE
	#
	session.execute("CREATE KEYSPACE IF NOT EXISTS exercise WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};")
	session.execute("USE exercise;")
	mylogger.info('KEYSPACE created or existed.')
	
	session.execute("DROP TABLE IF EXISTS flights;")
	#session.execute("CREATE TABLE flights (id int, year int, day_of_month int, fl_date timestamp, PRIMARY KEY (id));")
	session.execute(cql_create_table)
	mylogger.info('TABLE created.')
	
	#
	# prepare the INSERT statement
	#
	#stmt = session.prepare("INSERT INTO flights (id, year, day_of_month, fl_date) VALUES(?,?,?,?);")
	stmt = session.prepare(cql_insert_stmt)
	mylogger.info('INSERT statment prepared.')
	
	
	#
	# read and insert rows into table
	#
	datafile = 'flights_from_pg.csv'
	start_time = time.time()
	rows = 0
	rows_written = 0
	
	mylogger.info('Start file read and insert into C*.')
	with open(datafile, 'rb') as csv_file:
		reader=csv.DictReader(csv_file, field_names, delimiter=',', quotechar='"')
		try:
			for row in reader:
				if rows >= 0:
				#if rows < 5:
					rows += 1
					
					if rows == 1:
						batch = BatchStatement()
					if rows%100 == 0:
						session.execute_async(batch)
						rows_written = rows
						batch = BatchStatement()
						
					# nasty date time gymnastics ahead
					#
					# create a timestamp from fl_date to use in several places
					#
					fl_date = datetime.strptime(row['fl_date'], "%Y/%m/%d").replace(tzinfo=timezone('UTC'))
					#mylogger.info(fl_date)
					
					#
					# calculate departure date/time
					#
					time_str = ('0000' + row['dep_time'].strip())
					time_str = time_str[-4:]
					add_sec = int(time_str[:2]) * 60 * 60
					add_sec = add_sec + int(time_str[-2:]) * 60
					dep_time = fl_date + timedelta(0, add_sec)
					#mylogger.info(row['dep_time'], dep_time)
					
					#
					# calculate arrival date/time
					#
					time_str = ('0000' + row['arr_time'].strip())
					time_str = time_str[-4:]
					add_sec = int(time_str[:2]) * 60 * 60
					add_sec = add_sec + int(time_str[-2:]) * 60
					arr_time = fl_date + timedelta(0, add_sec)
					#mylogger.info(row['arr_time'], dep_time)
					
					#
					# insert the record
					#
					batch.add(stmt, \
					( \
					int(row['id']), \
					int(row['year']), \
					int(row['day_of_month']), \
					fl_date, \
					int(row['airline_id']), \
					row['carrier'], \
					int(row['fl_num']), \
					int(row['origin_airport_id']), \
					row['origin'], \
					row['origin_city_name'], \
					row['origin_state_abr'].strip(), \
					row['dest'], \
					row['dest_city_name'], \
					row['dest_state_abr'].strip(), \
					dep_time,\
					arr_time, \
					int(row['actual_elapsed_time']), \
					int(row['air_time']), \
					int(row['distance']), \
					int(row['air_time'])/10 \
					))
					
		except csv.Error as e:
			sys.exit("file%s, %s" % (datafile, e))
		
		if rows_written != rows:
			session.execute_async(batch)
			rows_written = rows
  
  	elapsed_time = time.time() - start_time
	mylogger.info('Read %d lines, wrote %d lines in %d seconds', rows, rows_written, elapsed_time)
	
	#
	# close and clean up the connection
	#
	session.cluster.shutdown()
	mylogger.info('Connection closed.')
	
	mylogger.info('End main()')
	
if __name__ == "__main__":
	main()
		