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
* Follow the instructions of the project documentation pdf file in order to set up rabbitmq, storm, ant etc.
* This project comes as a maven project so download an install [maven](http://maven.apache.org/download.cgi)
* Edit the properties in [storm.properties](../master/storm.properties) according to your local storm installation
* invoke "make build" in order to build the project and "make run" in order to run the REST service

## Installing a topology
* Copy a folder with the topology source code into the [topology source directory](../master/topology_src)
* Choose a name for the topology and name the folder according to this name (the name should be URL compatible)
* Each topology has to have its own ant build script which specifies how the topology is packaged into a jar etc. 
  (the ant target which deploys the topology to the storm cluster should match the name of the topology)
* For an example of a topology with an existing ant script see the project documentation pdf
  (You can follow the steps to configure this example topology and then just copy the folder into the [topology source directory](../master/topology_src) but make sure you change the name of the ant target which deploys the topology to
the name of the copied folder)
