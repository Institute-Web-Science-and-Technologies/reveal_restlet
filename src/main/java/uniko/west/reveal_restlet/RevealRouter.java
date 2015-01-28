/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Restlet;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.resource.Directory;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;

/**
 *
 * @author nico
 */
public class RevealRouter extends ServerResource {

    public static void main(String[] args) throws Exception {
        // Create a new Restlet component and add a HTTP server connector to it
        Component component = new Component();
        component.getServers().add(Protocol.HTTP, 8182);

        Router router = new Router(component.getContext().createChildContext());

        // Attach the resources to the router
        router.attach("/storm/{topology}/{action}", TopologyActionResource.class);
        router.attach("/manage", ManageResource.class);
        
        component.getDefaultHost().attach(router);

        component.start();

    }

}
