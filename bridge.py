import sys
import json
import logging
import subprocess
import os
import time
import argparse
import random
import math
import requests 
import pygeohash as pgh

from datetime import datetime as dt
from subprocess import PIPE, Popen


# Constants
script_basename = os.path.basename(__file__)
onos_poll_interval = 6 # Seconds to wait between ONOS Rest Api requests
path_to_kibana_ddl = "./kibana_objects.json"
path_to_elastic_index_schemas = "./elastic_index_schemas.json"
path_to_elastic_create_index = "./elastic_create_index.json"
json_header = {"Content-Type" : "application/json"}
kibana_header = {"kbn-xsrf" : "true"}
kibana_header.update(json_header)

# lat and longitude ranges for st lucia
long_start = 153.006618
long_end = 153.016595
lat_start = -27.494037
lat_end = -27.501456

# ONOS end point to elasticsearch index mapping.
onos_to_elastic = {
"/links":"links",
"/flows":"flows",
"/devices":"devices"
}

elastic = {"name":"elasticsearch", "host":"localhost", "port":"9200", "protocol":"http://"}
kibana = {"name":"kibana", "host" : "localhost", "port" : "5601", "protocol":"http://"}
onos = {"name":"onos", "host": "localhost", "port":"8181", "protocol":"http://"}

elastic_url = "{0:s}{1:s}:{2:s}".format(elastic['protocol'], elastic['host'], elastic['port'])
kibana_url = "{0:s}{1:s}:{2:s}".format(kibana['protocol'], kibana['host'], kibana['port'])
onos_url = "{0:s}{1:s}:{2:s}".format(onos['protocol'], onos['host'], onos['port'])

elastic_indexes = [elastic_index for onos_resource, elastic_index in onos_to_elastic.iteritems()]

parser = argparse.ArgumentParser()
parser.add_argument('--drop_all', help='Drops all existing elasticsearch indexes.')
args = vars(parser.parse_args())

with open(path_to_kibana_ddl, "r") as f:
	kibana_ddl = json.loads(f.read())
kibana_ddl_dict = {}
kibana_ddl_dict['objects'] = json.loads(kibana_ddl)
kibana_ddl = json.dumps(kibana_ddl_dict)

sys.exit(1)

with open(path_to_elastic_create_index, "r") as f:
	create_index = f.read()

with open(path_to_elastic_index_schemas, 'r') as f:
	schemas = f.read()
	elastic_index_to_schema = json.loads(schemas)

def wait_for_next_scrape(seconds):
    curr_secs = 0
    while curr_secs < seconds:
        logger.info("Seconds till next ingest from ONOS REST API: " + str(seconds - curr_secs) + "        ")
        sys.stdout.write("\033[F")
        sys.stdout.flush()
        time.sleep(1)
        curr_secs = curr_secs + 1

def pdumps(_dict):
	return json.dumps(_dict, sort_keys=True, indent=4, separators=(',', ': '))

def get_coordinate_within_square(_lat_start, _lat_end, _long_start, _long_end, current_points):
	generated_points = [(random.uniform(_lat_start, _lat_end), random.uniform(_long_start, _long_end)) for x in range(5)]
	#pick the point that has the biggest distance to its closest neighbour, this is just so the demonstration looks better, less crammed
	biggest_dist_to_nearest_neighbour = 0
	point_with_biggest_dist_to_nearest_neighbour = None
	for point_lat, point_long in generated_points:
		if len(current_points) == 0:
			return (point_lat, point_long)
		if point_with_biggest_dist_to_nearest_neighbour == None:
			point_with_biggest_dist_to_nearest_neighbour = (point_lat, point_long)
			continue
		dist_to_nearest_neighbour = max([math.hypot(point_lat - neighbour_lat, point_long - neighbour_long) for neighbour_lat,neighbour_long in current_points])
		if dist_to_nearest_neighbour > biggest_dist_to_nearest_neighbour:
			point_with_biggest_dist_to_nearest_neighbour = (point_lat, point_long)
			biggest_dist_to_nearest_neighbour = dist_to_nearest_neighbour
	return point_with_biggest_dist_to_nearest_neighbour

def drop_all_elastic_indexes(elastic_indexes, elastic_url):
	for elastic_index in elastic_indexes:
		url = "{0:s}/{1:s}".format(elastic_url, elastic_index)
		req = requests.delete(url)
		logger.info("Response content:\n" + pdumps(req.json()))
	logger.info("Succesfully dropped all Elasticsearch indexes.")	

def get_existing_elastic_indexes(_elastic_url):
	existing_elastic_indexes = set()
	req = requests.get("{0:s}/_cat/indices?".format(_elastic_url))
	for record in req.content.strip().split('\n'):
    		NAME_INDEX = 2
    		existing_elastic_indexes.add(record.split()[NAME_INDEX])
	return existing_elastic_indexes

def init_logger(logger_name):
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-8s %(message)s', "%H:%M:%S")
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	return logger

# Configure logging.
logger = init_logger(script_basename)
logger.info("Script starting.")

if args['drop_all']:
	drop_all_elastic_indexes(elastic_indexes, elastic_url)

# Initialise elasticsearch indexes.
existing_elastic_indexes = get_existing_elastic_indexes(elastic_url)

logger.info("About to create elastic indexes if they do not exist in Elasticsearch.")

for elastic_index in elastic_indexes:
    if elastic_index not in existing_elastic_indexes:
        logger.info("Index {0:s} does not exist in elasticsearch.".format(elastic_index))
	url = "{0:s}/{1:s}".format(elastic_url, elastic_index)
	req = requests.put(url, data = create_index, headers = json_header)
	logger.info(pdumps({"content":req.content, "headers" : str(req.headers), "data" : create_index}))

# Deploy the schemas for each index.
for elastic_index, schema in elastic_index_to_schema.iteritems():
	url = "{0:s}/{1:s}/_mapping/_doc".format(elastic_url, elastic_index)
	req = requests.put(url, data = schema, headers = json_header)
	logger.info(pdumps({"content":req.content, "headers" : str(req.headers), "data" : schema}))

logger.info("About to deploy Kibana dashboard.")
time.sleep(2)

# Import dashboard into Kibana, overwrite any existing dashboard with same ID
proc = Popen('curl -X POST "http://{0:s}:{1:s}/api/kibana/dashboards/import?force=true"'.format(kibana['host'],kibana['port']) + \
" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d'\n" + kibana_ddl + "\n'", stdout=PIPE, stderr=PIPE, shell=True)
(out, err) = proc.communicate()
if proc.returncode <> 0:
	logger.error(err)
	sys.exit(1)
print out
logger.info("Succesfully deployed Kibana Dashboard.")
time.sleep(2)

# now that elasticsearch and kibana services are running, and required indexes exist, scrape ONOS rest api every 30 seconds,
# enrich data with extract timestamp, and post document to relevant index
# The script may fail if elasticsearch or kibana services become unavailable.

logger.info("About to begin polling ONOS REST API")
ingest_start_time = int(time.time())
# Store last snapshot of flow in dictionary.
flows = {}
current_locations = set()
device_locations = {}
while True:
    # request each resource in the onos_to_elastic mapping, and post to elastic index
    # Set the extract timestamp
    extract_ts = int(time.time())
    for resource, index in onos_to_elastic.iteritems():
	if index != "flows":
		continue
        print '=' * 40 + ">"
        logger.info("ONOS API Resource request:\n" + \
        "curl -H 'Accept: application/json' -u onos:rocks -X GET 'http://{0:s}:{1:s}/onos/v1{2:s}'".format(onos['host'], onos['port'], resource))
        proc = Popen("curl -H 'Accept: application/json' -u onos:rocks -X GET 'http://{0:s}:{1:s}/onos/v1{2:s}'".format(onos['host'], onos['port'], resource), stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = proc.communicate()
        if proc.returncode <> 0:
            print err
        results_dict = json.loads(out)
	
	# Create an individual document for each child object returned in the response object.
	for child_object in results_dict[index]:
		# Enrich the data with a timestamp
		child_object['extract_timestamp'] = extract_ts
		# Enrich the data with a count since the start of the ingest process, used because Kibana makes it easier to filter by long range rather than date range??
		child_object['time_passed_seconds'] = extract_ts - ingest_start_time

		# Enrich data with location
		if index == "flows":
			#If device not already assigned a location, generate one
			if child_object['deviceId'] not in device_locations:
				
				geo_point = get_coordinate_within_square(lat_start, lat_end, long_start, long_end, current_locations)
				current_locations.add(geo_point)
				geo_hash = pgh.encode(geo_point[0], geo_point[1], precision=11)
				device_locations[child_object['deviceId']] = geo_hash
				child_object['location'] = geo_hash
			else:
				#get the device location
				child_object['location'] = device_locations[child_object['deviceId']]
		print str(current_locations)

		# Even though a flow may not be permanent and may only have a time to live of 10 seconds before it is removed from the flow tables, the flow if it is reinstalled
		# will have the same flow ID. The heatmap view of aggregate flow rule bytes per device requires a field which gives bytes per poll increment rather than a total.
		# This mean we be visualising the deltas, rather than the total, so we should enrich the data with a new field called bytes_delta for each flow.
		# This field therefore will only be populated after we have recieved at least two snapshots of a flow.		
		if index == "flows":
			# If first time we have seen the flow rule
			if child_object['id'] not in flows:
				flows[child_object['id']] = child_object
			# If we have the last snapshot of the flow
			else:
				# Compute the delta, add it to the flow object
				child_object['bytes_delta_' + str(onos_poll_interval)] = child_object['bytes'] - flows[child_object['id']]['bytes']
				# Update last known snapshot
				flows[child_object['id']] = child_object



		# Pretty print the results
		logger.info("Succesfully retrived object from ONOS resource " + resource +":\n" + json.dumps(child_object, sort_keys=True, indent=4, separators=(',', ': ')))

		# now post the data to elasticsearch
		proc = Popen("curl -X POST \"{0:s}:{1:s}/{2:s}/_doc/\" -H 'Content-Type: application/json' -d'".format(elastic["host"],elastic["port"],index) + json.dumps(child_object) + "'", stdout=PIPE, stderr=PIPE, shell=True)
		(out, err) = proc.communicate()
		if proc.returncode <> 0:
		    print err
		logger.info(out)
		logger.info(err)
		# Pretty print the results of the insertion into Elasticsearch
		logger.info("Succesfully inserted document from ONOS resource " + resource + " into Elasticsearch:\n" + json.dumps(json.loads(out), sort_keys=True, indent=4, separators=(',', ': ')))
		print "<" + '=' * 40

	# Although flow objects will retain the same ID when readded, the bytes counter is reset to zero, therefore we must purge any old flow objects,
	# otherwise we will end up with negative delta values.
	flows = {flow_id:flow for flow_id, flow in flows.iteritems() if flow['time_passed_seconds'] == extract_ts - ingest_start_time}
    wait_for_next_scrape(onos_poll_interval)

