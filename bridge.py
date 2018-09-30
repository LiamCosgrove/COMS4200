import sys, json, logging, subprocess, os, time, argparse, random
from datetime import datetime as dt
from subprocess import PIPE, Popen
import pygeohash as pgh
import math

# Constants

script_basename = os.path.basename(__file__)
# Seconds to wait between ONOS Rest Api requests
onos_poll_interval = 6

# lat and longitude ranges for st lucia
long_start = 153.006618
long_end = 153.016595
lat_start = -27.494037
lat_end = -27.501456

parser = argparse.ArgumentParser()
parser.add_argument('--drop_all', help='Drops all existing elasticsearch indexes.')
args = vars(parser.parse_args())



# Exported Kibana dashboard, import this into kibana
kibana_dashboard = '''
[
  {
    "_id": "1fe5e290-c474-11e8-ba05-ed8a23ede795",
    "_type": "dashboard",
    "_source": {
      "title": "myDashboard",
      "hits": 0,
      "description": "",
      "panelsJSON": "[
  {
    "embeddableConfig": {
      "mapCenter": [
        -27.497175340093158,
        153.01096916198733
      ],
      "mapZoom": 16
    },
    "gridData": {
      "h": 15,
      "i": "1",
      "w": 24,
      "x": 0,
      "y": 0
    },
    "id": "60115510-c470-11e8-ba05-ed8a23ede795",
    "panelIndex": "1",
    "type": "visualization",
    "version": "6.4.1"
  },
  {
    "embeddableConfig": {},
    "gridData": {
      "h": 15,
      "i": "2",
      "w": 24,
      "x": 24,
      "y": 0
    },
    "id": "5f6926d0-c473-11e8-ba05-ed8a23ede795",
    "panelIndex": "2",
    "type": "visualization",
    "version": "6.4.1"
  }
]",
      "optionsJSON": "{
  "darkTheme": false,
  "hidePanelTitles": false,
  "useMargins": true
}",
      "version": 1,
      "timeRestore": false,
      "kibanaSavedObjectMeta": {
        "searchSourceJSON": "{
  "query": {
    "language": "lucene",
    "query": ""
  },
  "filter": []
}"
      }
    }
  }
]
'''

create_index ='''
{
    "settings" : {
           "index" : {
        "number_of_shards" : 3, 
        "number_of_replicas" : 2 
        }
    }
}'''


def wait_for_next_scrape(seconds):
    curr_secs = 0
    while curr_secs < seconds:
        logger.info("Seconds till next ingest from ONOS REST API: " + str(seconds - curr_secs) + "        ")
        sys.stdout.write("\033[F")
        sys.stdout.flush()
        time.sleep(1)
        curr_secs = curr_secs + 1

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
	

# ONOS end point to elasticsearch index.
onos_to_elastic = {"/links":"links",
"/flows":"flows",
"/devices":"devices"
}

elastic_index_to_schema = {
"flows" : '{"properties":{"appId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"location":{"type":"geo_point"},"bytes":{"type":"long"},"deviceId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"extract_timestamp":{"type":"date"},"time_passed_seconds":{"type":"long"},"groupId":{"type":"long"},"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"isPermanent":{"type":"boolean"},"lastSeen":{"type":"long"},"life":{"type":"long"},"liveType":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"packets":{"type":"long"},"priority":{"type":"long"},"selector":{"properties":{"criteria":{"properties":{"ethType":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}}},"state":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"tableId":{"type":"long"},"timeout":{"type":"long"},"treatment":{"properties":{"clearDeferred":{"type":"boolean"},"instructions":{"properties":{"port":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}}}}}',

"links" : '{"properties":{"dst":{"properties":{"device":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"port":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"extract_timestamp":{"type":"date"},"time_passed_seconds":{"type":"long"},"src":{"properties":{"device":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"port":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"state":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}',

"devices" : '{"properties":{"annotations":{"properties":{"channelId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"managementAddress":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"protocol":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"available":{"type":"boolean"},"chassisId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"driver":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"time_passed_seconds":{"type":"long"},"extract_timestamp":{"type":"date"},"hw":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"location":{"type":"geo_point"},"mfr":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"role":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"serial":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"sw":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}'
}

elastic = {"name":"elasticsearch", "host":"localhost", "port":"9200"}
kibana = {"name":"kibana", "host" : "localhost", "port" : "5601"}
onos = {"name":"onos", "host": "localhost", "port":"8181"}
        
# Initialise.

# Configure logging.

logger = logging.getLogger(script_basename)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-8s %(message)s', "%H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.info("Script starting.")

if args['drop_all']:
    for resource, index in onos_to_elastic.iteritems():
        proc = Popen("curl -X DELETE '{0:s}:{1:s}/{2:s}'".format(elastic['host'], elastic['port'], index), stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = proc.communicate()
        if proc.returncode <> 0:
            logger.error(err)
            sys.exit(1)
    logger.info("Succesfully dropped all existing Elasticsearch indexes.")

# Create each Kibana index pattern.

# Drop then create all required Kibana Index-patterns.
logger.info("About to drop then create all required Kibana index-patterns.")
for resource, index in onos_to_elastic.iteritems():
    proc = Popen('curl -X DELETE "http://{0:s}:{1:s}/api/saved_objects/index-pattern/{2:s}"'.format(kibana['host'],kibana['port'], index) + \
    " -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d' " + '{"attributes": { "title": "' + index + '"}}' + "'", stdout=PIPE, stderr=PIPE, shell=True)
    (out, err) = proc.communicate()
    if proc.returncode <> 0:
        logger.error(err)
        sys.exit(1)
    proc = Popen('curl -X POST "http://{0:s}:{1:s}/api/saved_objects/index-pattern/{2:s}"'.format(kibana['host'],kibana['port'], index) + \
    " -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d' " + '{"attributes": { "title": "' + index + '"}}' + "'", stdout=PIPE, stderr=PIPE, shell=True)
    (out, err) = proc.communicate()
    if proc.returncode <> 0:
        logger.error(err)
        sys.exit(1)
logger.info("Succesfully installed all Kibana Index-patterns.")

# This script will not respawn services, but may inform user that a service is not running.

# Initialise elasticsearch indexes.

existing_elastic_indexes = set()
logger.info("About to list the existing elasticsearch indexes.")
proc = Popen("curl -X GET '{0:s}:{1:s}/_cat/indices?'".format(elastic['host'], elastic['port']), stdout=PIPE, stderr=PIPE, shell=True)
(out, err) = proc.communicate()
if proc.returncode <> 0:
    pass
logger.info("Existing elasticsearch indexes:\n" + out)
for record in out.strip().split('\n'):
    NAME_INDEX = 2
    existing_elastic_indexes.add(record.split()[NAME_INDEX])

# if the elastic index is not in the set of existing elastic indexes, then create it
for resource, index in onos_to_elastic.iteritems():
    if index not in existing_elastic_indexes:
        logger.info("Index " + index + " does not exist in elasticsearch.")
        print 'curl -X PUT "' + elastic["host"] + ":" + elastic["port"] + "/" + index + "\" -H 'Content-Type: application/json' -d'" + create_index + "\n'"
        proc = Popen('curl -X PUT "' + elastic["host"] + ":" + elastic["port"] + "/" + index + "\" -H 'Content-Type: application/json' -d'" + create_index + "\n'",\
        stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = proc.communicate()
        if proc.returncode <> 0:
            logger.critical("Failed to create elastic search index:" + index + ".")
            raise RuntimeError(err)
        logger.info("Successfully created elasticsearch index " + index + ".")


# Deploy the schemas for each index.
for index, schema in elastic_index_to_schema.iteritems():
	proc = Popen('curl -X PUT "{0:s}:{1:s}/'.format(elastic['host'], elastic['port']) + index + '/_mapping/_doc" -H \'Content-Type: application/json\' -d\'\n' + schema + "\n'", stdout=PIPE, stderr=PIPE, shell=True)
	(out, err) = proc.communicate()
	logger.info(err)
	logger.info(out)
	if proc.returncode <> 0 or "error" in err:
	    logger.error("Failed to deploy schema for index:" + index)
	    logger.error(err)
	    sys.exit(1)
	logger.info("Successfully deployed schema for index: " + index)

'''
# Import dashboard into Kibana, overwrite any existing dashboard with same ID
print 'curl -X POST "http://{0:s}:{1:s}/api/kibana/dashboards/import?force=true"'.format(kibana['host'],kibana['port']) + \
" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d' " + kibana_dashboard + "'"


proc = Popen('curl -X POST "http://{0:s}:{1:s}/api/kibana/dashboards/import?force=true'.format(kibana['host'],kibana['port']) + \
" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d' " + kibana_dashboard + "'", stdout=PIPE, stderr=PIPE, shell=True)
(out, err) = proc.communicate()
if proc.returncode <> 0:
	logger.error(err)
	sys.exit(1)

logger.info("Succesfully deployed Kibana Dashboard.")
time.sleep(2)
'''
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

