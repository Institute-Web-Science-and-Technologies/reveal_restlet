/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift7.TException;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 *
 * @author nico
 */
public class TopologyActionResource extends ServerResource {

    String topologyId;
    String action;
    String channelId;
    String propertyFilePath = "/home/nico/storm-example-v1_0/config/storm_config.ini";

    @Override
    public void doInit() {
        this.topologyId = getAttribute("topology");
        this.action = getAttribute("action");
        this.channelId = getQueryValue("channel");
        switch (action) {
            case "stop":
                stopTopology();
                break;
            case "start":
                startTopology();
                break;
            case "kill":
                killTopology();
                break;
            case "deploy":
                deployTopology();
                break;
        }
    }

    @Get(value = "txt")
    public String toString() {
        return "Topology : \"" + this.topologyId + "\"\n Action : " + this.action
                + "\n on Channel : " + channelId;
    }

    private void stopTopology() {
        Map conf = Utils.readStormConfig();
        Client client = NimbusClient.getConfiguredClient(conf).getClient();
        try {
            client.deactivate(topologyId);
        } catch (NotAliveException | TException ex) {
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void startTopology() {
        Map conf = Utils.readStormConfig();
        Client client = NimbusClient.getConfiguredClient(conf).getClient();
        try {
            client.activate(topologyId);
        } catch (NotAliveException | TException ex) {
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void killTopology() {
        Map conf = Utils.readStormConfig();
        Client client = NimbusClient.getConfiguredClient(conf).getClient();
        try {
            client.killTopology(topologyId);
        } catch (NotAliveException | TException ex) {
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void deployTopology() {
        try {
            AntRunner.deployTopology(topologyId, channelId);
        } catch (Exception ex) {
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
