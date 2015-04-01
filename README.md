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
* invoke "make run" or "mvn clean install" in order to run the REST service
* If topologies should be deployed to a remote server then the value for "nimbus.host" in [ant.properties](../master/ant.properties) has to be changed to the address of the remote server.
Furthermore the value for "restlet.url" has to be changed to the restlet server's address.


## Installing a topology
* Copy a folder with the topology source code into the [topology source directory](../master/topology_src)
* Choose a name for the topology and name the folder according to this name (the name should be URL compatible)
* Each topology has to be an own maven project and can therfore use maven to manage its dependencies
* For an example of a topology with an existing pom see [the west topology](https://github.com/nico1510/westTopology)

## Implementing a storm bolt
* Sample implementation are available [here](https://github.com/nico1510/westTopology/tree/master/src/main/java/uniko/west/westtopology/bolts)
* Some of the samples use static files for their computations. All static files which are used by a bolt have to be moved into the [resources](../master/resources) directory in order to be available at runtime. Every file which is located in this directory will be accessible via http at http://localhost:8182/static/${filename}. 
* In order to make third-party libraries available at runtime for storm bolts they have to be included in the 
topology jar ("fat jar" or "jar with dependencies"). This does not apply to storm libraries. Their scope has to
be set to "provided" in order to exclude them from being packaged. See the [pom](https://github.com/nico1510/westTopology/blob/master/pom.xml) of the west topology for an example.
