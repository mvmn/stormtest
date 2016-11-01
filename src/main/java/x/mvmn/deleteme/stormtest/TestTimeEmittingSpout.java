package x.mvmn.deleteme.stormtest;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TestTimeEmittingSpout extends BaseRichSpout {
	private static final long serialVersionUID = 7588302194343339186L;

	protected SpoutOutputCollector collector;
	protected final Random random = new Random();

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		final String timeStr = (random.nextFloat() > 0.9 ? "surprise! " : "") + System.currentTimeMillis();
		collector.emit(new Values(timeStr), timeStr);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time"));
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("Got ack: " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.err.println("Got fail: " + msgId);
		final String timeStr = msgId.toString();
		collector.emit(new Values(timeStr), timeStr);
	}
}
