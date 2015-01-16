/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import backtype.storm.generated.KillOptions;
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

    @Override
    public void doInit() {
        this.topologyId = getAttribute("topology");
        this.action = getAttribute("action");
        this.channelId = getQueryValue("channel");
        if(action.equals("stop")) {
            stopTopology();
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
        } catch (NotAliveException ex) {
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        } catch (TException ex) {
            Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
