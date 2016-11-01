package x.mvmn.deleteme.stormtest;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TestDateFormattingBolt extends BaseRichBolt {
	private static final long serialVersionUID = 9165021065699662323L;

	protected final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected OutputCollector collector;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try {
			final String inputStr = input.getString(0);
			try {
				collector.emit("sucessess", input, new Values(sdf.format(new Date(Long.parseLong(inputStr)))));
			} catch (NumberFormatException e) {
				// ExceptionUtils.getFullStackTrace(e);
				collector.emit("errors", input, new Values("Error occurred - failed to parse " + inputStr));
			}
			collector.ack(input);
		} catch (Throwable t) {
			collector.fail(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("sucessess", new Fields("timeformatted"));
		declarer.declareStream("errors", new Fields("stacktrace"));
	}
}
