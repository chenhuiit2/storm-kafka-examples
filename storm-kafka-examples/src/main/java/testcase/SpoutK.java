package testcase;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoutK {
	public static class ExclamationBolt extends BaseRichBolt {
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			System.out.println(tuple.getString(0));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

	}
    /** 
     * ByteBuffer 转换 String 
     * @param buffer 
     * @return 
     */  
    public static String getString(ByteBuffer buffer)  
    {  
        Charset charset = null;  
        CharsetDecoder decoder = null;  
        CharBuffer charBuffer = null;  
        try  
        {  
            charset = Charset.forName("UTF-8");  
            decoder = charset.newDecoder();  
            // charBuffer = decoder.decode(buffer);//用这个的话，只能输出来一次结果，第二次显示为空  
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());  
            return charBuffer.toString();  
        }  
        catch (Exception ex)  
        {  
            ex.printStackTrace();  
            return "";  
        }  
    } 
	public static class TestMessageScheme implements Scheme {

		private static final Logger LOGGER = LoggerFactory.getLogger(TestMessageScheme.class);

		public List<Object> deserialize(ByteBuffer ser) {
			try {
				
				String msg = new String(getString(ser));
				return new Values(msg);
			} catch (Exception e) {
				LOGGER.error("Cannot parse the provided message!");
			}

			// TODO: what happend if returns null?
			return null;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("msg");
		}

	}

	public static void main(String[] args) {
		String topic = "test";
		String zkRoot = "";
		String spoutId = "test_spout_id";
		BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new TestMessageScheme());

		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
		builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("spout");

		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setDebug(false);
        /*Map<String, String> map=new HashMap<String,String>();
        // 配置Kafka broker地址
        map.put("metadata.broker.list", "127.0.0.1:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);*/
		
		StormTopology topo = builder.createTopology();
		cluster.submitTopology("topo", conf, topo);
		Utils.sleep(1000000);
		cluster.killTopology("topo");
		cluster.shutdown();

	}

}
