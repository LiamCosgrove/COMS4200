import sys, json, logging, subprocess, os, time, argparse
from datetime import datetime as dt
from subprocess import PIPE, Popen

# Constants

script_basename = os.path.basename(__file__)
# Seconds to wait between ONOS Rest Api requests
onos_poll_interval = 30

parser = argparse.ArgumentParser()
parser.add_argument('--drop_all', help='Drops all existing elasticsearch indexes.')
args = vars(parser.parse_args())


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

# ONOS end point to elasticsearch index.
onos_to_elastic = {"/links":"links",
"/flows":"flows",
"/devices":"devices"
}

elastic_index_to_schema = {
"flows" : '{"properties":{"extract_timestamp":{"type":"date"},"flows":{"properties":{"appId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"bytes":{"type":"long"},"deviceId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"groupId":{"type":"long"},"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"isPermanent":{"type":"boolean"},"lastSeen":{"type":"long"},"life":{"type":"long"},"liveType":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"packets":{"type":"long"},"priority":{"type":"long"},"selector":{"properties":{"criteria":{"properties":{"ethType":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}}},"state":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"tableId":{"type":"long"},"timeout":{"type":"long"},"treatment":{"properties":{"clearDeferred":{"type":"boolean"},"instructions":{"properties":{"port":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}}}}}}}',

"links" : '{"properties":{"links":{"properties":{"dst":{"properties":{"device":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"port":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"src":{"properties":{"device":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"port":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"state":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"extract_timestamp":{"type":"date"}}}',

"devices" : '{"properties":{"devices":{"properties":{"annotations":{"properties":{"channelId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"managementAddress":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"protocol":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"available":{"type":"boolean"},"chassisId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"driver":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"location":{"type":"geo_point"},"hw":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"mfr":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"role":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"serial":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"sw":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"type":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"extract_timestamp":{"type":"date"}}}'
}


device_id_to_geohash = {"of:0000000000000001": "r7hg9fdghmc0",
"of:0000000000000002" : "r7hgd40ht47u",
"of:0000000000000003": "r7hg9fun8mgu",
"of:0000000000000004" : "r7hg9fxz0uet"}

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

# now that elasticsearch and kibana services are running, and required indexes exist, scrape ONOS rest api every 30 seconds,
# enrich data with extract timestamp, and post document to relevant index
# The script may fail if elasticsearch or kibana services become unavailable.

logger.info("About to begin polling ONOS REST API")

while True:
    
    # request each resource in the onos_to_elastic mapping, and post to elastic index
    # Set the extract timestamp
    extract_ts = int(time.time())
    for resource, index in onos_to_elastic.iteritems():
        print '=' * 40 + ">"
        logger.info("ONOS API Resource request:\n" + \
        "curl -H 'Accept: application/json' -u onos:rocks -X GET 'http://{0:s}:{1:s}/onos/v1{2:s}'".format(onos['host'], onos['port'], resource))
        proc = Popen("curl -H 'Accept: application/json' -u onos:rocks -X GET 'http://{0:s}:{1:s}/onos/v1{2:s}'".format(onos['host'], onos['port'], resource), stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = proc.communicate()
        if proc.returncode <> 0:
            print err
        results_dict = json.loads(out)

        # Enrich the data with a timestamp

        results_dict['extract_timestamp'] = extract_ts

        # Perform resource specific enrichment
        if index == "devices":
            for device in results_dict['devices']:
                device['location'] = device_id_to_geohash[device['id']]
        # Pretty print the results
        
        logger.info("Succesfully retrived ONOS resource:\n" + json.dumps(results_dict, sort_keys=True, indent=4, separators=(',', ': ')))

        # now post the data to elasticsearch
        proc = Popen("curl -X POST \"{0:s}:{1:s}/{2:s}/_doc/\" -H 'Content-Type: application/json' -d'".format(elastic["host"],elastic["port"],index) + json.dumps(results_dict) + "'", stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = proc.communicate()
        if proc.returncode <> 0:
            print err
	logger.info(out)
	logger.info(err)
        # Pretty print the results of the insertion into Elasticsearch
        logger.info("Succesfully inserted ONOS resource into Elasticsearch:\n" + json.dumps(json.loads(out), sort_keys=True, indent=4, separators=(',', ': ')))
        print "<" + '=' * 40
    wait_for_next_scrape(onos_poll_interval)

