package x.mvmn.deleteme.stormtest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class StormTest {

	public static void main(String[] args) throws Exception {
		final String topologyName = "MvmnStormTestTopology";
		Config config = new Config();
		config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 5);
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("datetimes", new TestTimeEmittingSpout(), 1).setNumTasks(1);
		topologyBuilder.setBolt("dateFormat", new TestDateFormattingBolt()).shuffleGrouping("datetimes");
		topologyBuilder.setBolt("print", new TestPrintingBolt("GOOD")).shuffleGrouping("dateFormat", "sucessess")
				.shuffleGrouping("datetimes");
		topologyBuilder.setBolt("printError", new TestPrintingBolt("ERROR")).shuffleGrouping("dateFormat", "errors");

		if (args.length > 0 && args[0].equals("local")) {
			new LocalCluster().submitTopology(topologyName, config, topologyBuilder.createTopology());
		} else {
			StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());
		}
	}
}
