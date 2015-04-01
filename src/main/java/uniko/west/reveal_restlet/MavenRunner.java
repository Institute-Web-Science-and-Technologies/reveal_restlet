/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.Invoker;

/**
 *
 * @author nico
 */
public class MavenRunner {

    public static void deployTopology(String topologyName, String exchangeName) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream(StaticInformation.deploymentPropertiesFile));
        props.setProperty("exchange.name", exchangeName);
        File pomFile = new File(StaticInformation.topologySrcDir + File.separator + topologyName + File.separator + "pom.xml");

        InvocationRequest request = new DefaultInvocationRequest();
        request.setPomFile(pomFile);
        request.setGoals(Arrays.asList("clean", "install"));
        request.setProperties(props);
        request.setBaseDirectory(new File(StaticInformation.topologySrcDir + File.separator + topologyName));

        Invoker invoker = new DefaultInvoker();
        invoker.execute(request);
    }
}
