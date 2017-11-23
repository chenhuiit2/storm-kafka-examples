package biz.click;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RepeatVisitBolt extends BaseRichBolt{
	private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector) {
		
		this.collector=_collector;
	}

	@Override
	public void execute(Tuple input) {
		String userID = input.getStringByField("user_id");
		String _id=input.getStringByField("_id");
		String merchantID = input.getStringByField("merchant_id");
		
		String key= userID + " " + merchantID;
		Click _click = ClickHelper.get(key);
		if(_click == null) {
			ClickHelper.put(key, new Click());
			collector.emit(new Values(key,merchantID,"1"));
			
		} else {
			collector.emit(new Values(key,merchantID,"0"));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("key","merchant_id","is_first"));
	}

}
