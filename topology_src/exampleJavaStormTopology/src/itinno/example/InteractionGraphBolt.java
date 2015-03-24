/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package itinno.example;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import itinno.common.StormLoggingHelper;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author nico
 */
public class InteractionGraphBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String strExampleEmitFieldsId;
    private boolean initialized = false;

    private DateTime deadline;
    private int intervalInMinutes = 60;
    private DateTime bufferStartTime = null;
    private HashMap<String, HashMap<String, HashSet<String>>> interactionGraph = new HashMap<>();

    // Initialise Logger object
    private ch.qos.logback.classic.Logger logger = null;
    private String strLogBaseDir;
    private String strLogPattern;
    private ch.qos.logback.classic.Level logLevel;

    public InteractionGraphBolt(String strExampleEmitFieldsId, String strLogBaseDir, String strLogPattern, ch.qos.logback.classic.Level logLevel) throws Exception {
        super();

        // Store emit fields name, ExampleSocialMediaJavaLoggerBolt id and path to the main configuration file
        if (strExampleEmitFieldsId.isEmpty()) {
            throw new Exception("Emit fields id can not be nil or emmty.");
        }

        // Check if the log file name length is more than 0
        if (strLogBaseDir.isEmpty()) {
            throw new Exception("Log bolt file name can not be empty,");
        }

        // Check if logging pattern is more than 0
        if (strLogPattern.isEmpty()) {
            throw new Exception("Logging pattern can not be empty,");
        }

        // Check if the log Level is instance of (be explicit here about the level in order to make sure that correct instance being checked) 
        if (!(logLevel instanceof ch.qos.logback.classic.Level)) {
            throw new Exception("Log level object must be instance of the ch.qos.logback.classic.Level, but was ." + logLevel.getClass());
        }

        // After all the above checks complete, store the emit field id, path (or name) of the log file and log level  
        this.strExampleEmitFieldsId = strExampleEmitFieldsId;
        this.strLogBaseDir = strLogBaseDir;
        this.strLogPattern = strLogPattern;
        this.logLevel = logLevel;
    }

    /**
     * Prepare method is similar the "Open" method for Spouts and is called when
     * a worker is about to be put to work. This method also initialise the main
     * example Storm Java bolt.
     *
     * @param stormConf map of the storm configuration (passed within Storm
     * topology itself, not be a user)
     * @param context context (e.g. similar to description) of the topology
     * (passed within Storm topology itself, not be a user)
     * @param collector output collector of the Storm (which is responsible to
     * emiting new tuples, passed within Storm topology itself, not be a user)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {

        // set up the logger
        String strPID = null;

        // Setup the logger
        try {
            // Create log file name - combination of class name and current PID e.g. ExampleJavaSocialMediaStormTopologyRunner_pid123.log
            try {
                // Try to get the pid using java.lang.Management class and split it on @ symbol (e.g. returned value will be in the format of {p_id}@{host_name})
                strPID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

                // Handle any possible exception here, such as if the process_name@host will not be returned (possible, depends on different JVMs)
            } catch (Exception e) {
                // Print the message, stacktrace and allow to continue (pid value will not be contained in the log file)
                System.err.println("Failed to get process process id. Will continue but the log files names will not contain pid value. Details: " + e.getMessage());
                e.printStackTrace();

                // Pid will be simply an empty value
                strPID = "";
            }

            // Create log file name - combination of class name and current process id, e.g. ExampleSocialMediaJavaLoggerBolt_pid123.log 
            String strLogName = "LocationCrawlerBolt_pid" + strPID + ".log";

            // Specify the path to the log file (the file that will be created)
            String fileSep = System.getProperty("file.separator");
            String strLogFilePath = this.strLogBaseDir + fileSep + strLogName;

            StormLoggingHelper stormLoggingHelper = new StormLoggingHelper();
            this.logger = stormLoggingHelper.createLogger(InteractionGraphBolt.class.getName(), strLogFilePath,
                    this.strLogPattern, this.logLevel);

            // Issue test message
            this.logger.info("Logger was initialised.");

        } catch (Exception e) {
            // Print error message, stacktrace and throw an exception since the log functionality is the main target of this bolt 
            System.err.printf("Error occurred during Storm Java Logger Bolt logger setup. Details: " + e.getMessage());
            e.printStackTrace();
            try {
                throw new Exception("Java Storm logger bolt log initialisation failed. Details: " + e.getMessage());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * searches for locations in the message and computes related locations
     *
     * @param input standard Storm tuple input object (passed within Storm
     * topology itself, not be a user)
     */
    @Override
    public void execute(Tuple input) {
        // Retrieve hash map tuple object from Tuple input at index 0, index 1 will be message delivery tag (not used here)
        Map<Object, Object> inputMap = (HashMap<Object, Object>) input.getValue(0);
        // Get JSON object from the HashMap from the Collections.singletonList
        JSONObject message = (JSONObject) Collections.singletonList(inputMap.get("message")).get(0);
        
        if(!message.containsKey("created_at")) {
            return;     // skip delete messages
        }
        
        // Print received message
//        this.logger.info("Received message: " + message.toJSONString());

        DateTime timestamp = DateTime.parse((String) message.get("created_at"), DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.US));
        
        if (bufferStartTime == null) {
            bufferStartTime = timestamp;
            deadline = bufferStartTime.plusMinutes(intervalInMinutes);
        }
        
        String authorId = (String) ((JSONObject) message.get("user")).get("id_str");

        if (!interactionGraph.containsKey(authorId)) {
            interactionGraph.put(authorId, new HashMap<String, HashSet<String>>());
        }
        HashMap<String, HashSet<String>> authorActions = interactionGraph.get(authorId);

        countReplies(message, authorActions);
        countMentions(message, authorActions);
        countRetweets(message, authorActions);

        if (timestamp.isAfter(deadline) || timestamp.isEqual(deadline)) {
            deadline.plusMinutes(intervalInMinutes);
            ObjectMapper mapper = new ObjectMapper();
            String jsonResult;
            try {
                Map<String, Object> jsonResultObject = new HashMap();
                jsonResultObject.put("start", bufferStartTime);
                jsonResultObject.put("end", timestamp);
                jsonResultObject.put("result", interactionGraph);
                jsonResult = mapper.writeValueAsString(jsonResultObject);
                this.logger.info("final result= " + jsonResult);
                this.collector.emit(new Values(jsonResult));
                // Acknowledge the collector that we actually received the input
                collector.ack(input);
                this.interactionGraph = new HashMap<>();
                this.bufferStartTime = null;
            } catch (JsonProcessingException ex) {
                Logger.getLogger(InteractionGraphBolt.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
        }
    }

    private void countReplies(JSONObject message, HashMap<String, HashSet<String>> authorActions) {
        String replyId = (String) message.get("in_reply_to_user_id_str");

        if (replyId != null) {
            if (!authorActions.containsKey("replied_to")) {
                authorActions.put("replied_to", new HashSet<String>());
            }
            authorActions.get("replied_to").add(replyId);
        }
    }

    private void countMentions(JSONObject message, HashMap<String, HashSet<String>> authorActions) {
        JSONArray userMentions = (JSONArray) ((JSONObject) message.get("entities")).get("user_mentions");
        if (userMentions != null) {
            if (!authorActions.containsKey("mentioned")) {
                authorActions.put("mentioned", new HashSet<String>());
            }
            for (Object o : userMentions) {
                String mentionedUser = (String) ((JSONObject) o).get("id_str");
                authorActions.get("mentioned").add(mentionedUser);
            }
        }
    }

    private void countRetweets(JSONObject message, HashMap<String, HashSet<String>> authorActions) {
        JSONObject retweetStatus = (JSONObject) message.get("retweeted_status");
        if (retweetStatus != null) {
            if (!authorActions.containsKey("retweeted")) {
                authorActions.put("retweeted", new HashSet<String>());
            }
            String authorId = (String) ((JSONObject) retweetStatus.get("user")).get("id_str");
            authorActions.get("retweeted").add(authorId);
        }
    }

    /**
     * Declare output field name (in this case simple a string value that is
     * defined in the constructor call)
     *
     * @param declarer standard Storm output fields declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(strExampleEmitFieldsId));
    }

}
