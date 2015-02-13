/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import java.io.File;
import org.restlet.Component;
import org.restlet.data.Protocol;
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
        component.getClients().add(Protocol.FILE);

        Router router = new Router(component.getContext().createChildContext());

        // Attach the resources to the router
        router.attach("/storm/{topology}/{action}", TopologyActionResource.class);
        router.attach("/manage", ManageResource.class);

        File resourcesDir = new File("./resources");
        Directory directory = new Directory(component.getContext().createChildContext(), "file://" + resourcesDir.getAbsolutePath());
        router.attach("/static/", directory);

        component.getDefaultHost().attach(router);

        component.start();

    }

}
