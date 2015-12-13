/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.reveal_restlet;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift7.TException;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 *
 * @author nico
 */
public class TopologyActionResource extends ServerResource {

	String topologyFolderName;
	String topologyName;
	String action;
	String exchangeName;

	String status;
	boolean success;

	@Override
	public void doInit() {
		this.topologyFolderName = this.getAttribute("topology");
		this.action = this.getAttribute("action");
		this.exchangeName = this.getQueryValue("exchange");
		this.topologyName = this.exchangeName + "_" + this.topologyFolderName;
		Map conf = Utils.readStormConfig();
		Nimbus.Client client;
		try {
			client = NimbusClient.getConfiguredClient(conf).getClient();
		} catch (Exception ex) {
			this.success = false;
			this.status = "Failed to connect to Nimbus!";
			Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
			return;
		}
		switch (this.action) {
		case "stop":
			this.stopTopology(client);
			break;
		case "start":
			this.startTopology(client);
			break;
		case "kill":
			this.killTopology(client);
			break;
		case "deploy":
			this.deployTopology();
			break;
		}
	}

	@Get(value = "json")
	@Override
	public String toString() {
		ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
		responseObject.put("success", this.success);
		responseObject.put("status", this.status);

		return responseObject.toString();
	}

	private void stopTopology(Client client) {
		try {
			if (this.exchangeName != null) {
				client.deactivate(this.topologyName);
				this.success = true;
				this.status = "Topology " + this.topologyName + " is now inactive";
			} else {
				this.success = false;
				this.status = "The exchange parameter needs to be set with: .../stop?exchange=<exchange-name>";
			}
		} catch (NotAliveException | TException ex) {
			this.success = false;
			Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void startTopology(Client client) {
		try {
			if (this.exchangeName != null) {
				client.activate(this.topologyName);
				this.success = true;
				this.status = "Topology " + this.topologyName + " is now active";
			} else {
				this.success = false;
				this.status = "The exchange parameter needs to be set with: .../start?exchange=<exchange-name>";
			}
		} catch (NotAliveException | TException ex) {
			this.success = false;
			Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void killTopology(Client client) {
		try {
			if (this.exchangeName != null) {
				KillOptions killOpts = new KillOptions();
				killOpts.set_wait_secs(2);
				client.killTopologyWithOpts(this.topologyName, killOpts);
				this.success = true;
				this.status = "Topology " + this.topologyName + " was killed";
			} else {
				this.success = false;
				this.status = "The exchange parameter needs to be set with: .../kill?exchange=<exchange-name>";
			}
		} catch (NotAliveException | TException ex) {
			this.success = false;
			Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void deployTopology() {
		try {
			if (this.exchangeName != null) {
				MavenRunner.deployTopology(this.topologyFolderName, this.exchangeName);
				this.success = true;
				this.status = "Topology " + this.topologyName + " was deployed and is listening to exchange "
						+ this.exchangeName;
			} else {
				this.success = false;
				this.status = "The exchange parameter needs to be set with: .../deploy?exchange=<exchange-name>";
			}
		} catch (Exception ex) {
			this.success = false;
			this.status = "topology " + this.topologyName
					+ " is already deployed to the cluster or was not found in directory "
					+ StaticInformation.topologySrcDir;
			Logger.getLogger(TopologyActionResource.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
