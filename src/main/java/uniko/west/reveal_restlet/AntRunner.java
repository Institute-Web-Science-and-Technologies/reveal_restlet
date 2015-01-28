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
        props.load(new FileInputStream(StaticInformation.stormPropertiesFile));
        File buildFile = new File(StaticInformation.topologySrcDir + File.separator + topologyName + File.separator + "build.xml");
        Project p = new Project();
        p.setUserProperty("ant.file", buildFile.getAbsolutePath());
        p.setProperty("build.compiler", "extJavac");    // this is a fix see : http://stackoverflow.com/a/15409110/1644061
        p.init();
        ProjectHelper helper = ProjectHelper.getProjectHelper();
        p.setProperty("channelId", exchangeName);
        
        for (Entry<Object, Object> prop : props.entrySet()) {
            p.setProperty((String) prop.getKey(), (String) prop.getValue());
        }
        
        p.addReference("ant.projectHelper", helper);
        helper.parse(p, buildFile);
        p.executeTarget(topologyName);
    }

}
