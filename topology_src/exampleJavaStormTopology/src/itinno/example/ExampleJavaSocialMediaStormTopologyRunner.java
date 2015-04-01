/////////////////////////////////////////////////////////////////////////
//
// \xa9 University of Southampton IT Innovation, 2014
//
// Copyright in this software belongs to IT Innovation Centre of
// Gamma House, Enterprise Road, Southampton SO16 7NS, UK.
//
// This software may not be used, sold, licensed, transferred, copied
// or reproduced in whole or in part in any manner or form or in or
// on any media by any person other than in accordance with the terms
// of the Licence Agreement supplied with the software, or otherwise
// without the prior written consent of the copyright owners.
//
// This software is distributed WITHOUT ANY WARRANTY, without even the
// implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE, except where stated in the Licence Agreement supplied with
// the software.
//
//	Created By :	Vadim Krivcov
//	Created Date :	2014/03/27
//	Created for Project:	REVEAL
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////
package itinno.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

// Import core Storm classes
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

// Import AMQP Connection Configuration classes
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
import io.latent.storm.rabbitmq.Declarator;

// Import main RabbitMQ broker communication classes
import com.rabbitmq.client.ConnectionFactory;

// Import third party SimpleJSONScheme class
import com.rapportive.storm.scheme.SimpleJSONScheme;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main Java STORM Runner class
 *
 * NOTE: There is API documentation (if available) provided in order to help
 * understanding the Storm and its configurations/processes, but the API
 * documentation is out-dated (provides API for Storm v.0.8.1, but Storm is
 * 0.9.1), however it is still consistent in most of the cases.
 *
 * Main STORM API (v.0.8.1):
 * http://nathanmarz.github.io/storm/doc-0.8.1/index.html (unfortunately there
 * is not direct link to specific APIs)
 *
 */
public class ExampleJavaSocialMediaStormTopologyRunner {

    // General (brief) help instructions on how to start Java/Python storm and corresponding unit tests
    private static String strStormRunInstructions = "\n\n\nGeneral example Storm help instructions:"
            + "\n--------------------\n"
            + "\n\nDeploy example Storm topology"
            + "\n--------------------"
            + "\nDeploy example Java Storm topology (example Storm topology with Java bolts only):"
            + "\n - Windows OS command: ant -f build.xml example-storm-java -propertyfile=\"storm.properties\""
            + "\n - Unix OS command:    ant -f build.xml example-storm-java -propertyfile storm.properties"
            + "\n\nDeploy example Python Storm topology (example Storm topology with Java and Python bolts):"
            + "\n - Windows OS command: ant -f build.xml example-storm-python -propertyfile=\"storm.properties\""
            + "\n - Unix OS command:    ant -f build.xml example-storm-python -propertyfile storm.properties"
            + "\n\n\nUnit tests (send example message containing json object to the rabbitmq)"
            + "\n--------------------"
            + "\nUnit test using java client:"
            + "\n - Windows OS command: ant -f build.xml example-client-java -propertyfile=\"storm.properties\""
            + "\n - Unix OS command:    ant -f build.xml example-client-java -propertyfile storm.properties"
            + "\n\nUnit test using python client:"
            + "\n - Windows and Unix OS commands: ant -f build.xml example-client-python"
            + "\n\n";

    public static void main(String[] args) {
        // Local topology cluster
        LocalCluster clusterLocalTopology;

        // Topology builder
        TopologyBuilder builder; // OK

        // Storm Spouts
        IRichSpout stormExampleSocialMediaAMQPSpout; // OK
        SpoutDeclarer spoutDeclarer; // OK

        // Storm bolts
        BoltDeclarer boltDeclarer;
//                LocationCrawlerBolt locationCrawlerBolt;
        DiscussionTreeBolt discussionTreeBolt;
        InteractionGraphBolt interactionGraphBolt;

        // Customer configuration 
        ConsumerConfig stormSocialMediaSpoutConfig = null; // OK

        // Customer configuration builder
        ConsumerConfigBuilder stormSocialMediaSpoutConfigBuilder = null; // OK

        // RabbitMQ Connection Configuration
        ConnectionConfig stormSocialMediaSpoutRabbitMQconnectionConfig = null;  // OK

        // Simple Storm tuple JSON Scheme
        SimpleJSONScheme socialMediaScheme = null; // OK

        // Storm RabbitMQ queue declarator
        Declarator declarator;

        // RabbitMQ configuration file variables
        String strRMQHost = null;
        int nRMQPort = 0;
        String strRMQUsername = null;
        String strRMQPassword = null;
        int nRMQHeartBeaat = 0;
        String strRMQQueueName = null;
        String strRMQExchange = null;
        String strRMQExchangeType = null;
        String strRMQRouting = null;

        // Storm Topology, Spout and Bolts IDs variables
        String strExampleSocialMediaAMQPSpoutId = null;
        String strExampleSocialMediaClientFrameworkStreamId = null;
        String strExampleEmitFieldsId = null;

        // URL of restlet service 
        String restletURL = null;

        // Storm Topology configuration parameters
        boolean bTopologyDebug = false;

        // Storm Spout configuration parameters
        boolean bSpoutDebug = false;
        int nRabbitMQPrefetch = 0;
        int nMaxSpoutPending = 0;

        // Main Storm Social Media Properties file
        File fileConfigFile = null;

        // Create Properties builder object (e.g. storm properties file should be passed as a command line argument)
        Properties properties = new Properties();

        // Command line arguments (configuration file and Storm cluster mode)
        String strPropFileLocation = null;
        String strStormClusterMode = null;

        // Parse all configuration command line arguments
        try {
            // First of all get the length of command line arguments 
            int nArgsLength = args.length;

            /* First of all need to check the number of command line arguments (minimum number of arguments should be 4) e.g.
             * -config configuration_file.ini and -mode local/distributed (total count of the arguments is 4)
             */
            if (nArgsLength < 5) {
                throw new IllegalArgumentException("Some of the configuration command line arguments were invalid or were not specified. Please refer to the Storm help menu.");
            }

            /* If Storm mode argument was specified, then check if local or distributed mode was requested
             * 	- First of all need to check if the command line argument contained "=" character (e.g. mode=local),
             * 	- Secondly need to check if a valid mode was specified. Valid modes are "local" or "distributed" 
             */
            int argsLenght = args.length;
            String[] arguments = new String[argsLenght];

            // Helper local variables that will flag if the "-config" and "-mode" arguments were specified 
            boolean bConfigArgument = false;
            boolean bModeArgument = false;

            for (int i = 0; i < arguments.length; i++) {
                // Check if the configuration file (e.g. -config) command line arguments was specified
                if (args[i].equals("-config")) {
                    // Set boolean flag indicating that the "-config" command line argument was specified
                    bConfigArgument = true;
                    System.out.println("Detected config");
                    /* Check the length of the configuration file (minimum length is 4, e.g. a.ini), as well as check if the configuration file 
                     * contains ".ini" (as an extension) 
                     */
                    String strTempConfigFile = args[++i];

                    if (strTempConfigFile.length() >= 4 && strTempConfigFile.contains(".ini")) {
                        // Storm the configuration file (or path to the configuration file)
                        strPropFileLocation = strTempConfigFile;

                        // Otherwise raise an exception if the length of the configuration file is less than 4 and the configuration file does not contain ".ini" 
                    } else {
                        throw new IllegalArgumentException("Configuration file is incorrect or was not specified. Please refer to general Storm help instructions.");
                    }

                    // If the mode (e.g. -mode) command line arguments was specified
                }
                if (args[i].equals("-mode")) {
                    // Set boolean flag indicating that the "-mode" command line argument was specified
                    bModeArgument = true;

                    /* Check the length of the mode (minimum length is 5, e.g. local), as well as check if the mode description string 
                     * equals to either "local" or "distributed"
                     */
                    String strTempMode = args[++i];

                    // Check if the length of the mode is minimum 5 (e.g. local)
                    if (strTempMode.length() >= 5) {
                        // Finally check if the mode is equal to "local" or "distributed"
                        if (strTempMode.toLowerCase().equals("local") || strTempMode.toLowerCase().equals("distributed")) {
                            strStormClusterMode = strTempMode;

                            // If mode does not match "local" or "distributed" then raise an Exception
                        } else {
                            throw new IllegalArgumentException("Storm cluster mode is invalid. Valid Storm modes are local or distributed (e.g. -mode local).");
                        }

                        // Raise an exception if the length of mode is less that 5 and is the mode string contains any special characters 
                    } else {
                        throw new IllegalArgumentException("Storm cluster mode is invalid or was not specified. Please refer to general Storm help instructions.");
                    }
                }
                if (args[i].equals("-channelId")) {
                    strRMQExchange = args[++i];
                }
                if (args[i].equals("-restletURL")) {
                    restletURL = args[++i];
                }
            }

            /* After all the command line arguments were parsed, double check if the "-config" and "-mode" command line arguments were specified
             * It is a little cumbersome to check them in the above block
             */
            if (bConfigArgument == false) {
                throw new IllegalArgumentException("Main Storm configuration file was not specified. Please refer to general Storm help instructions.");
            }

            if (bModeArgument == false) {
                throw new IllegalArgumentException("Main Storm mode was not specified. Please refer to general Storm help instructions.");
            }

            // Create temporary configuration file
            fileConfigFile = new File(strPropFileLocation);

            // Check if the file exists and set its permission
            if (fileConfigFile.exists()) {
                if (!fileConfigFile.canExecute()) {
                    fileConfigFile.setExecutable(true);
                }
            } else {
                throw new FileNotFoundException("File doesn't exists at the specified path: " + strPropFileLocation);
            }

            // Create Java properties file from the passed configuration file
            properties.load(new FileInputStream(fileConfigFile));

        } catch (IOException e) {
            // Print error message, stacktrace and exit
            System.err.printf(e.getMessage());
            e.printStackTrace();
            System.out.println(strStormRunInstructions);
            System.exit(1);

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Exception occurred during configuration file loading. "
                    + "\n\nDetails: %s.", e.getMessage()
                    + strStormRunInstructions);
            e.printStackTrace();
            System.exit(1);
        }

        // Get all the needed RabbitMQ connection properties from the configuration file
        try {
            strRMQHost = properties.getProperty("rmqhost", "localhost");
            nRMQPort = Integer.parseInt(properties.getProperty("rmqport", "5672"));
            strRMQUsername = properties.getProperty("rmqusername", "guest");
            strRMQPassword = properties.getProperty("rmqpassword");
            nRMQHeartBeaat = Integer.parseInt(properties.getProperty("rmqheartbeat", "10"));
            strRMQQueueName = properties.getProperty("rmqqueuename", "test");
//			strRMQExchange = properties.getProperty( "rmqexchange", "test-exchange" );
            strRMQExchangeType = properties.getProperty("rmqexchangetype", "topic");
            strRMQRouting = properties.getProperty("rmqrouting", "test-routing");

            // Get all the needed Storm Topology, Spout and Bolts IDs from the configuration file
            strExampleSocialMediaAMQPSpoutId = properties.getProperty("example_spout_amqp_spout_id", "exampleSocialMediaAMQPSpout");
            strExampleSocialMediaClientFrameworkStreamId = properties.getProperty("example_java_storm_topology_id", "exampleJavaStormTopology");
            strExampleEmitFieldsId = properties.getProperty("example_emit_fields_id", "word");

            // Get Storm Topology configuration parameters
            bTopologyDebug = Boolean.valueOf(properties.getProperty("topology_debug", "false"));

            // Get Storm Spout configuration parameters
            bSpoutDebug = Boolean.valueOf(properties.getProperty("spout_debug", "false"));
            nRabbitMQPrefetch = Integer.parseInt(properties.getProperty("spout_rmqprefetch", "200"));
            nMaxSpoutPending = Integer.parseInt(properties.getProperty("spout_max_spout_pending", "200"));

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Error occurred during main STORM configuration file parsing. Please refer to the instructions in provided in the configuration file.\nDetails: %s.", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

		// Try to setup main Java Storm Topology runner logger 
        // Create SocialMediaSpout Configuration Builder, create Topology builder, set spouts/bolts and start execute the topology
        try {
			// Create Storm object Scheme (default encoding is utf-8, but others can be passed to the constructor). 
			/* API: http://code.rapportive.com/storm-json/doc/com/rapportive/storm/scheme/SimpleJSONScheme.html
             * 
             */
            socialMediaScheme = new SimpleJSONScheme();

            /* Create RabbitMQ connection configuration
             * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "RabbitMQ Spout") 
             */
            stormSocialMediaSpoutRabbitMQconnectionConfig = new ConnectionConfig(strRMQHost, nRMQPort, strRMQUsername, strRMQPassword,
                    ConnectionFactory.DEFAULT_VHOST, nRMQHeartBeaat);
            Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Initialised RabbitMQ connection configuration object.");
            /* Create Storm Spout configuration builder
             * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "RabbitMQ Spout")
             */
            stormSocialMediaSpoutConfigBuilder = new ConsumerConfigBuilder();
            stormSocialMediaSpoutConfigBuilder.connection(stormSocialMediaSpoutRabbitMQconnectionConfig);
            stormSocialMediaSpoutConfigBuilder.queue(strRMQQueueName);
            stormSocialMediaSpoutConfigBuilder.prefetch(nRabbitMQPrefetch);
            stormSocialMediaSpoutConfigBuilder.requeueOnFail();
            Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Initialised Spout configuration builder.");

            /* Build Storm spout configuration
             * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "RabbitMQ Spout")
             */
            stormSocialMediaSpoutConfig = stormSocialMediaSpoutConfigBuilder.build();
            Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Initialised Spout configuration builder.");

            /* Create a AMQP Declarator (will declare queue if it does not exist on the time of the Storm launch)
             * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "Declarator")
             */
            declarator = new ExampleSocialMediaStormDeclarator(strRMQExchange, strRMQExchangeType, strRMQRouting, strRMQQueueName);

            /* Initialise Social Media Spout
             * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "IRichSpout")
             */
            stormExampleSocialMediaAMQPSpout = new ExampleSocialMediaAMQPSpout(socialMediaScheme, declarator);
            Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Initialised AMQP Spout object on exchange " + strRMQExchange);

            /* Create a simple STORM topology configuration file
             * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "Config")
             */
            Config conf = new Config();
            conf.put(Config.TOPOLOGY_DEBUG, bTopologyDebug);
            conf.setDebug(bTopologyDebug);
            Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Initialised main example Storm confuration.");

            /* Initialise Storm Topology
             * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "TopologyBuilder")
             */
            builder = new TopologyBuilder();

            /* Define a new Spout in the topology
             * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "SpoutDeclarer")
             */
            spoutDeclarer = builder.setSpout(strExampleSocialMediaAMQPSpoutId, stormExampleSocialMediaAMQPSpout);
            Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Declared AMQP Spout to the example Storm topology.");

            // Add configuration to the StoputDeclarer
            spoutDeclarer.addConfigurations(stormSocialMediaSpoutConfig.asMap());

            /* Explanation taken from: https://github.com/ppat/storm-rabbitmq
             * Set MaxSpoutPending value to the same value as RabbitMQ pre-fetch count (set initially in in the ConsumerConfig above). It is possible
             * to tune them later separately, but MaxSpoutPending should always be <= Prefetch 
             */
            spoutDeclarer.setMaxSpoutPending(nMaxSpoutPending);
            spoutDeclarer.setDebug(bSpoutDebug);

//                        locationCrawlerBolt = new LocationCrawlerBolt(strExampleEmitFieldsId, restletURL );
//                        boltDeclarer = builder.setBolt( "locationCrawlerId", locationCrawlerBolt );
//			boltDeclarer.shuffleGrouping( strExampleSocialMediaAMQPSpoutId );
            discussionTreeBolt = new DiscussionTreeBolt(strExampleEmitFieldsId);
            boltDeclarer = builder.setBolt("discussionTeeBoltId", discussionTreeBolt);
            boltDeclarer.shuffleGrouping(strExampleSocialMediaAMQPSpoutId);

            interactionGraphBolt = new InteractionGraphBolt(strExampleEmitFieldsId);
            boltDeclarer = builder.setBolt("interactionGraphBoltId", interactionGraphBolt);
            boltDeclarer.shuffleGrouping(strExampleSocialMediaAMQPSpoutId);

            // Check configuration boolean value "bLocalTopology" and decide whether to start Local Topology cluster or submit the Topology to the distributed cluster
            if (strStormClusterMode.equals("local")) {
                // Deploy the topology on the Local Cluster (e.g. local mode)
                clusterLocalTopology = new LocalCluster();
                clusterLocalTopology.submitTopology(strExampleSocialMediaClientFrameworkStreamId, conf, builder.createTopology());

            } else if (strStormClusterMode.equals("distributed")) {
                // Submit the topology to the distribution cluster that will be defined in Storm client configuration file or via cmd as a parameter ( e.g. nimbus.host=localhost )
                StormSubmitter.submitTopology(strExampleSocialMediaClientFrameworkStreamId, conf, builder.createTopology());
                Logger.getLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName()).log(Level.INFO, "Submitted the example Storm topology.");

            } else {
                throw new RuntimeException("Unknown Storm mode was specified. Valid modes are local or distributed, which should be specified as cmd argument. "
                        + "Please refer to general Storm help instructions.");
            }

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Exception occurred during Storm topology start. Details: %s.\n", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
