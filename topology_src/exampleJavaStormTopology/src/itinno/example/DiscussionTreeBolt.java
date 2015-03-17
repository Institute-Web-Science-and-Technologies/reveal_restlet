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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONObject;

/**
 *
 * @author nico
 */
public class DiscussionTreeBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String strExampleEmitFieldsId;
    private boolean initialized = false;

    private DateTime deadline;
    private int intervalInMinutes;
    private HashMap<String, HashSet<String>> tweetIdSetsMap = new HashMap<>();
    private HashMap<String, ArrayList<Tweet>> discussionTreesMap = new HashMap<>();

    // Initialise Logger object
    private ch.qos.logback.classic.Logger logger = null;
    private String strLogBaseDir;
    private String strLogPattern;
    private ch.qos.logback.classic.Level logLevel;

    public DiscussionTreeBolt(String strExampleEmitFieldsId, String strLogBaseDir, String strLogPattern, ch.qos.logback.classic.Level logLevel) throws Exception {
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
            this.logger = stormLoggingHelper.createLogger(DiscussionTreeBolt.class.getName(), strLogFilePath,
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

        // Print received message
        this.logger.info("Received message: " + message.toJSONString());

        DateTime timestamp = DateTime.parse((String) message.get("created_at"), DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.US));
        String authorId = (String) ((JSONObject) message.get("user")).get("id_str");
        String text = (String) message.get("text");
        String tweetId = (String) message.get("id_str");
        
        
        String ancestorTweetId = (String) message.get("in_reply_to_status_id_str");
        
        JSONObject retweeted_status = (JSONObject) message.get("retweeted_status");
        if(retweeted_status != null) {
            ancestorTweetId = (String) ((JSONObject) message.get("retweeted_status")).get("id_str");
        }
        
        Tweet tweet = new Tweet(authorId, tweetId, timestamp, text, ancestorTweetId);

        if (ancestorTweetId != null) {
            boolean found = false;
            for(Entry<String, HashSet<String>> e : tweetIdSetsMap.entrySet()) {
                if(e.getValue().contains(ancestorTweetId)) {
                    e.getValue().add(tweetId);
                    tweetIdSetsMap.get(ancestorTweetId).add(tweet.tweet_id);
                    discussionTreesMap.get(ancestorTweetId).add(tweet);
                    found=true;
                    break;
                }
            }
            if(!found) {
                // tweet is a reply or retweet but its ancestor was'nt observed by this bolt, therefore it is treated as a new root Tweet
                tweetIdSetsMap.put(tweetId, new HashSet<>(Arrays.asList(tweetId)));
                discussionTreesMap.put(tweetId, new ArrayList<>(Arrays.asList(tweet)));
            }
        } else {
            // tweet is no reply or retweet 
            tweetIdSetsMap.put(tweetId, new HashSet<>(Arrays.asList(tweetId)));
            discussionTreesMap.put(tweetId, new ArrayList<>(Arrays.asList(tweet)));
        }

        if (timestamp.isAfter(deadline) || timestamp.isEqual(deadline)) {
            deadline.plusMinutes(intervalInMinutes);
            ObjectMapper mapper = new ObjectMapper();
            String jsonResult;
            try {
                jsonResult = mapper.writeValueAsString(discussionTreesMap.values());
                this.logger.info("final result= " + jsonResult);
                this.collector.emit(new Values(jsonResult));
                // Acknowledge the collector that we actually received the input
                collector.ack(input);
                this.tweetIdSetsMap = new HashMap<>();
                this.discussionTreesMap = new HashMap<>();
            } catch (JsonProcessingException ex) {
                Logger.getLogger(DiscussionTreeBolt.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
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

    public class Tweet implements Comparable<Tweet> {
        private String author_id;
        private String tweet_id;
        private DateTime timestamp;
        private String text;
        private String in_reply_to;
        
        public Tweet(String authorId, String tweetId, DateTime timestamp, String text, String inReplyTo) {
            this.author_id = authorId;
            this.tweet_id = tweetId;
            this.timestamp = timestamp;
            this.text = text;
            this.in_reply_to = inReplyTo;
        }

        /**
         * @return the author_id
         */
        public String getAuthor_id() {
            return author_id;
        }

        /**
         * @param author_id the author_id to set
         */
        public void setAuthor_id(String author_id) {
            this.author_id = author_id;
        }

        /**
         * @return the tweet_id
         */
        public String getTweet_id() {
            return tweet_id;
        }

        /**
         * @param tweet_id the tweet_id to set
         */
        public void setTweet_id(String tweet_id) {
            this.tweet_id = tweet_id;
        }

        /**
         * @return the timestamp
         */
        public DateTime getTimestamp() {
            return timestamp;
        }

        /**
         * @param timestamp the timestamp to set
         */
        public void setTimestamp(DateTime timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * @return the text
         */
        public String getText() {
            return text;
        }

        /**
         * @param text the text to set
         */
        public void setText(String text) {
            this.text = text;
        }

        /**
         * @return the in_reply_to
         */
        public String getIn_reply_to() {
            return in_reply_to;
        }

        /**
         * @param in_reply_to the in_reply_to to set
         */
        public void setIn_reply_to(String in_reply_to) {
            this.in_reply_to = in_reply_to;
        }
        
        @Override
        public int compareTo(Tweet o) {
            return this.timestamp.compareTo(o.getTimestamp());
        }        
    }
}
