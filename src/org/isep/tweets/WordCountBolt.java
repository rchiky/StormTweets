package org.isep.tweets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

public class WordCountBolt extends BaseRichBolt {
	List<String> excluded = Lists.newArrayList("Christmas");
	OutputCollector _collector;

	
	@Override
	public void execute(Tuple input) {
		String tweetText = input.getString(2);
		
		
		//1. Filter out with stop words and count.
		Map<String, Integer> wordCountMap = new HashMap<String, Integer>();
		String [] words = tweetText.toLowerCase().split(" ");
	
		for(String word: words) {
			if((word.matches("^[a-z]+$")) 
					&& word.length() > 4
					&& !excluded.contains(word)) {
				int val = wordCountMap.containsKey(word) ? wordCountMap.get(word) + 1 : 1;
				wordCountMap.put(word, val);
			}
		}
		
		//2. emit results:
		for(Entry<String,Integer> e: wordCountMap.entrySet()) {
		    _collector.emit(input, new Values(e.getKey(), e.getValue()));
		}
	    _collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word", "count"));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		_collector = arg2;
		
	}    



}
