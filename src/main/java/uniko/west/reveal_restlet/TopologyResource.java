/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.ConnectException;
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
public class TopologyResource extends ServerResource {

    String topologyId;
    boolean success;
    String status;

    @Override
    public void doInit() {
        this.topologyId = getAttribute("topology");
        Map conf = Utils.readStormConfig();
        Nimbus.Client client;
        try {
            client = NimbusClient.getConfiguredClient(conf).getClient();
        } catch (Exception ex) {
            success = false;
            status = "Failed to connect to Nimbus!";
            Logger.getLogger(TopologyResource.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        try {
            if (client.getTopology(topologyId) != null) {
                success = true;
                status = "Topology " + topologyId + " found! Available actions : start/stop/kill/deploy";
            } else {
                success = false;
                status = "Topology " + topologyId + " not found!";
            }
        } catch (NotAliveException | TException ex) {
            success = false;
            status = "Server exception occurred!";
            Logger.getLogger(TopologyResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Get(value = "json")
    public String toString() {
        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
        responseObject.put("success", success);
        responseObject.put("status", status);

        return responseObject.toString();
    }
}
