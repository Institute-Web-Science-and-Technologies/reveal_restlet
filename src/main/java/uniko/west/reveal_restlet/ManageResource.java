/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 *
 * @author nico
 */
public class ManageResource extends ServerResource {

    @Get(value = "html")
    public String toString() {
        List<String> toplogyNames = fileList(StaticInformation.topologySrcDir);
        
        // list toplogy names
        String response = "<span><ul style=\"display:inline-block\">";
        for (String top : toplogyNames) {
            response += "<li>" + top + "</li>";
        }
        
        response += "</ul><ul style=\"display:inline-block\">";
        
        // now list links for different actions
        for (String top : toplogyNames) {
            response += "<li style=\"display:inline-block; padding-right:10px\"><a href=\"" + super.getRootRef() + "/storm/" + top + "/start" + "\">start</a></li>";
            response += "<li style=\"display:inline-block; padding-right:10px\"><a href=\"" + super.getRootRef() + "/storm/" + top + "/stop" + "\">stop</a></li>";
            response += "<li style=\"display:inline-block; padding-right:10px\"><a href=\"" + super.getRootRef() + "/storm/" + top + "/kill" + "\">kill</a></li>";
            response += "<li style=\"display:inline-block; padding-right:10px\"><a href=\"" + super.getRootRef() + "/storm/" + top + "/deploy" + "\">deploy</a></li>";
        }
        return response + "</ul></span>";
    }

    public static List<String> fileList(String directory) {
        List<String> fileNames = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path path : directoryStream) {
                if(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)){
                    fileNames.add(path.getFileName().toString());
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(ManageResource.class.getName()).log(Level.SEVERE, null, ex);
        }
        return fileNames;
    }
}
