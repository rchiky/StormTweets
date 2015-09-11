package org.isep.tweets;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MergeWordsRanks extends BaseRichBolt {

	private final int top, emitFrequency;
	private final String fileName;
	private TreeSet<TopWord> totalRanking;
	private File resultFile;

	public MergeWordsRanks(int top,int emitFrequency, String fileName) {
		this.top = top;
		this.fileName = fileName;
		this.emitFrequency = emitFrequency;
	}

	@Override
	public void execute(Tuple tuple) {
		if (Utils.isTickTuple(tuple)) {
			
			// 1. prune the top
			while (totalRanking.size() > top) {
				totalRanking.pollLast();
			}
			// 2. compute rates
			if (totalRanking.size() > 0) {
				double totalSum = 0.0;
				for (TopWord tw : totalRanking) {
					totalSum += tw.count;
				}
				
				
				
				Map<String, Double> result = computeRankings(totalSum);
				if (result != null)
					writeResultsToFile(result);

				// 4. clear the rankings
				totalRanking.clear();
			}
		} else {
			
			TreeSet<TopWord> intermediateRanking = (TreeSet<TopWord>) tuple.getValue(0);
			
			totalRanking.addAll(intermediateRanking);
		}

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.totalRanking = new TreeSet<TopWord>();
		this.resultFile = new File(fileName);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}
	
	 @Override
	 public Map<String, Object> getComponentConfiguration() {
		 Map<String, Object> conf = new HashMap<String, Object>();
		 conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
		 return conf;
	 }

	private Map<String, Double> computeRankings(double totalSum) {
		if (totalRanking.size() == 0) {
			return null;
		}
		Map<String, Double> resultMap = new LinkedHashMap<String, Double>();
		for (TopWord tw : totalRanking) {
			resultMap.put(tw.word, tw.count / totalSum);
		}

		return resultMap;

	}

	/*
	 * This method writes the given results to database (JSON File ???).
	 * Silently fails
	 */
	private void writeResultsToFile(Map<String, Double> resultMap) {
		// 1. Generate JSON string to write

		StringBuilder sb = new StringBuilder("[");

		for (Entry<String, Double> e : resultMap.entrySet()) {
			sb.append("{\"word\":\"" + e.getKey() + "\", \"rank\":" + e.getValue() + "},");
		}
		sb.replace(sb.length() - 1, sb.length(), "]");
		
		// 2. Write JSON result

		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(resultFile);
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

		if (fos != null) {
			try {
				java.nio.channels.FileLock lock = fos.getChannel().lock();
				try {
					
					Writer writer = new OutputStreamWriter(fos,
										Charset.forName("UTF-8"));
					System.out.println("Writing NOW !");
					writer.write(sb.toString());
					writer.flush();
				} finally {
					lock.release();
				}
			} catch (IOException e) {
				
				e.printStackTrace();
			} finally {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
