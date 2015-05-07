# reveal_restlet

## Description
A simple REST service for starting, stopping, killing and deploying topologies.
### API Usage (replace localhost by server name) :
action  | REST call
------------- | -------------
list all installed topologies | http://localhost:8182/manage
start someTopology  | http://localhost:8182/storm/someTopology/start
stop someTopology  | http://localhost:8182/storm/someTopology/stop
kill someTopology  | http://localhost:8182/storm/someTopology/kill
deploy someTopology on someChannel | http://localhost:8182/storm/someTopology/deploy?channel=someChannel

## Installing the REST service
* Follow the instructions of the project documentation pdf file in order to set up rabbitmq and storm
* This project comes as a maven project so download and install [maven](http://maven.apache.org/download.cgi)
* invoke `make run` or `mvn clean install` in order to run the REST service
* If topologies should be deployed to a remote server then the value for "nimbus.host" in [deployment.properties](../master/deployment.properties) has to be changed to the address of the remote server.
Furthermore the value for "restlet.url" has to be changed to the restlet server's address.

## Installing a topology
* Clone the [west topologies repo](https://github.com/Institute-Web-Science-and-Technologies/westTopologies) into the [topology source directory](../master/topology_src)
* Now follow the instructions for creating a topology [here](https://github.com/Institute-Web-Science-and-Technologies/westTopologies/blob/master/README.md#creating-a-new-topology)
