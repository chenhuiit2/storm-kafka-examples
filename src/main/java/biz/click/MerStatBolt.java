package biz.click;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MerStatBolt extends BaseRichBolt{
	private OutputCollector collector;
	private Map<String, Integer> merStat = new HashMap<String, Integer>();
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector) {
		
		this.collector=_collector;
	}

	@Override
	public void execute(Tuple input) {
		String userID = input.getStringByField("user_id");
		String _id=input.getStringByField("_id");
		String merchantID = input.getStringByField("merchant_id");
		
		
		Integer count = merStat.get(merchantID);
		
		if(count == null) {
			count = 1;
			merStat.put(merchantID, count);
		} else {
			count = count + 1;
			merStat.put(merchantID, count);
		}
		
		collector.emit(new Values(merchantID,count));
	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("merchant_id","mer_count"));
	}

}
