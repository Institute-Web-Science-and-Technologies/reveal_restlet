/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package itinno.example;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author nico
 */
public class Test {
    
    public static void main(String[] args) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        JSONObject message = (JSONObject)parser.parse(new FileReader("/home/nico/Downloads/example-tweet-utf8.json"));
         System.out.println(message.get("contributors") == "null");
    }
    
}
