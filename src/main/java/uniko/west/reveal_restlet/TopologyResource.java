/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uniko.west.reveal_restlet;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 *
 * @author nico
 */
public class TopologyResource extends ServerResource {
    
    String topologyId;

    @Override
    public void doInit() {
        this.topologyId = getAttribute("topology");
    }

    @Get(value = "txt")
    public String toString() {
        return "Topology : \"" + this.topologyId + "\"";
    }
}

