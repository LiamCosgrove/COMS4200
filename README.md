# COMS4200

Please note: It is expected that you will manually import the Kibana Dashboard into Kibana

The file to import is called "kibana_objects.json"

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/dashboard_photo.png)

To run the project:

Please install pygeohash:
https://pypi.org/project/pygeohash/

Start the Kibana, Elasticsearch, and ONOS services.

Go to the Kibana web UI ->
Click the management tab from the main menu ->
Click the Saved Objects button

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana1.png)

Click import button in the top right ->

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana2.png)

Upload the file ''kibana_objects.json'

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana3.png)

?? Kibana will prompt you to define a default index pattern ?? ->
Enter 'f*' as the default index pattern. -> 
Click the next step button.

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana4.png)

Select 'extract_timestamp' from the TimeFilter field name drop down. ->
Click the create index button.

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana5.png)

Go to the management tab from the main menu ->
Click the advanced settings button.

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana6.png)

Set the 'timelion:es:timefield' config value to 'extract_timestamp'.

![alt text](https://github.com/LiamCosgrove/COMS4200/blob/master/images/kibana_setup/kibana7.png)

