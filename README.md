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
* This project comes as a maven project so download and install [maven](http://maven.apache.org/download.cgi)
* Edit the properties in [ant.properties](../master/ant.properties) according to your local storm installation
* invoke "make build" in order to build the project and "make run" in order to run the REST service

## Installing a topology
* Copy a folder with the topology source code into the [topology source directory](../master/topology_src)
* Choose a name for the topology and name the folder according to this name (the name should be URL compatible)
* Each topology has to have its own ant build script which specifies how the topology is packaged into a jar etc. 
  (the ant target which deploys the topology to the storm cluster should match the name of the topology)
* For an example of a topology with an existing ant script see the project documentation pdf
  (You can follow the steps to configure this example topology and then just copy the folder into the [topology source directory](../master/topology_src) but make sure you change the name of the ant target which deploys the topology to
the name of the copied folder)

## Implementing a storm bolt
* A sample implementation is available at [here](../master/topology_src/exampleJavaStormTopology/src/itinno/example/LocationCrawlerBolt.java)
* This sample uses static files for its computations. All static files which are used by a bolt have to be moved into the [resources](../master/resources)
directory in order to be available at runtime. Every file which is located in this directory will be accessible via http at http://localhost:8182/static. 
If the topology runs on a different server than this REST service then the entry for "restlet.url" in [ant.properties](../master/ant.properties has to be edited accordingly.
* Other than static files, also third-party libraries have to be available at runtime for storm bolts. The [sample implementation](../master/topology_src/exampleJavaStormTopology/src/itinno/example/LocationCrawlerBolt.java)
for instance uses the apache Jena library. Therefore all library jars have to be copied into the $STORM_HOME/lib folder to make them available for storm bolts.
