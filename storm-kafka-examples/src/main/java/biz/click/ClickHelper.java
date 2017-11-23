package biz.click;

import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;

public class ClickHelper {
	static Map<String,Click> map = new ConcurrentHashMap<String,Click>();
	public static Click get(String key) {
		return map.get(key);
	}
	
	public static void put(String key, Click click) {
		map.put(key, click);
	}
	
}
