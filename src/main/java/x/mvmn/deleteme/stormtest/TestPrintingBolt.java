package x.mvmn.deleteme.stormtest;

import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class TestPrintingBolt extends BaseRichBolt {
	private static final long serialVersionUID = 3978269079013310360L;
	protected OutputCollector collector;
	protected final Random random = new Random();

	protected final String name;

	public TestPrintingBolt(final String name) {
		this.name = name;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try {
			final float randomFloat = random.nextFloat();
			if (randomFloat > 0.9) {
				System.out.println("Fail immediately: " + input.getString(0));
				// Fail immediately
				collector.fail(input);
			} else if (randomFloat > 0.8) {
				System.out.println("Fail by timeout: " + input.getString(0));
				// Fail by timeout - e.g. do nothing, neither fail nor ack
			} else {
				System.out.println("PrintingBolt " + name + " says: " + input.getString(0));
				collector.ack(input);
			}
		} catch (Throwable t) {
			collector.fail(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
