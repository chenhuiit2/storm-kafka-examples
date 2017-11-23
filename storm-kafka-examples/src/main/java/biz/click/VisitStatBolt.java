package biz.click;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class VisitStatBolt extends BaseRichBolt{
	private OutputCollector collector;
	private int total;
	private int uniqueTotal;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector) {
		
		this.collector=_collector;
	}

	@Override
	public void execute(Tuple input) {
		String is_first = input.getStringByField("is_first");
		total ++; 
		
		if("1".equals(is_first)) {
			uniqueTotal ++;
		}
		collector.emit(new Values(total,uniqueTotal));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("total","uniqueTotal"));
	}

}
