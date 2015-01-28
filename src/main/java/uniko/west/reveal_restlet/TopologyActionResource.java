/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    String status;
    boolean success;

    @Override
    public void doInit() {
        this.topologyId = getAttribute("topology");
        this.action = getAttribute("action");
        this.channelId = getQueryValue("channel");
        Map conf = Utils.readStormConfig();
        Nimbus.Client client;
        try {
            client = NimbusClient.getConfiguredClient(conf).getClient();
        } catch (Exception ex) {
            success = false;
            status = "Failed to connect to Nimbus!";
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        switch (action) {
            case "stop":
                stopTopology(client);
                break;
            case "start":
                startTopology(client);
                break;
            case "kill":
                killTopology(client);
                break;
            case "deploy":
                deployTopology();
                break;
        }
    }

    @Get(value = "json")
    public String toString() {
        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
        responseObject.put("success", success);
        responseObject.put("status", status);

        return responseObject.toString();
    }

    private void stopTopology(Client client) {
        try {
            client.deactivate(topologyId);
            success = true;
            status = "Topology " + topologyId + " is now inactive";
        } catch (NotAliveException | TException ex) {
            success = false;
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void startTopology(Client client) {
        try {
            client.activate(topologyId);
            success = true;
            status = "Topology " + topologyId + " is now active";
        } catch (NotAliveException | TException ex) {
            success = false;
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void killTopology(Client client) {
        try {
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(2);
            client.killTopologyWithOpts(topologyId, killOpts);
            success = true;
            status = "Topology " + topologyId + " was killed";
        } catch (NotAliveException | TException ex) {
            success = false;
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void deployTopology() {
        try {
            if (channelId != null) {
                AntRunner.deployTopology(topologyId, channelId);
                success = true;
                status = "Topology " + topologyId + " was deployed and is listening on channel " + channelId;
            } else {
                success = false;
                status = "channel must not be null";
            }
        } catch (Exception ex) {
            success = false;
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
