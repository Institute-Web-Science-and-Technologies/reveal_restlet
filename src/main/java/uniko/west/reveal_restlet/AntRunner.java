/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;

/**
 *
 * @author nico
 */
public class AntRunner {

    public static void deployTopology(String topologyName, String exchangeName) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("/home/nico/storm-example-v1_0/storm.properties"));
        File buildFile = new File("/home/nico/storm-example-v1_0/build.xml");
        Project p = new Project();
        p.setUserProperty("ant.file", buildFile.getAbsolutePath());
        p.init();
        ProjectHelper helper = ProjectHelper.getProjectHelper();
        p.setProperty("channelId", exchangeName);
//        p.setProperty("nimbus.host", "localhost");
//        p.setProperty("storm.home", "/home/nico/apache-storm-0.9.1-incubating");
        for (Entry<Object, Object> prop : props.entrySet()) {
            p.setProperty((String) prop.getKey(), (String) prop.getValue());
        }
        p.addReference("ant.projectHelper", helper);
        helper.parse(p, buildFile);
        p.executeTarget(topologyName);
    }

}
