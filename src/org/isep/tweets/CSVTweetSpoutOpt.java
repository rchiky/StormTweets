package org.isep.tweets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.StringTokenizer;

import com.google.common.io.Files;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CSVTweetSpoutOpt extends BaseRichSpout {
	
	public static int MAX_QUEUE_SZ= 200;
	public static int THRESHOLD = 100;
	

	private SpoutOutputCollector _collector;
	private final List<String> fileList;
	private List<BufferedReader> scanList = new ArrayList<BufferedReader>();
	private final Queue<Tweet> recQ = new PriorityQueue<Tweet>(MAX_QUEUE_SZ);
	
	
	public CSVTweetSpoutOpt(List<String> fileList) throws FileNotFoundException {
		this.fileList = fileList;
	}
	
	
	private void init(List<String> fileList) throws FileNotFoundException {
		for(String fileName: fileList) {
			File f  = new File(fileName);
			
			if(f.exists()) {
				System.out.println("OPENING: " + fileName);
				BufferedReader br = Files.newReader(f, Charset.forName("UTF-8"));
				scanList.add(br);
			}
		}
	}
	@Override
	public void nextTuple() {
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Refill the list
		if(recQ.size() < THRESHOLD ) { // Read from scanners.
			
			while(recQ.size() < MAX_QUEUE_SZ && scanList.size() > 0) {
				ListIterator<BufferedReader> it = scanList.listIterator();
				
				while(it.hasNext()) {
					
					BufferedReader br = it.next();
					
					String tweetStr = null;
					try {
						tweetStr = br.readLine();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if(tweetStr !=null) {
						Tweet t = scanTweet(tweetStr);
						if(t != null) recQ.offer(t);
						
					}else {
						System.out.println("Nothing to read");
						try {
							br.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						it.remove();
					}
				}
			}
		}
		
		// emit next tweet
		Tweet tweet = recQ.poll();
		if(tweet != null ) {
			System.out.println(tweet.getId() + " " + tweet.getText() );
			_collector.emit(new Values(tweet.getId(),tweet.getCreationDate(), tweet.getText()));
		}
	}

	private Tweet scanTweet(String  line) {
		
		StringTokenizer st = new StringTokenizer(line,";",false);
		Tweet t = null;
		try {
		 t = new Tweet(Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()), st.nextToken());
		} catch (Exception e) {
			
		}
		return t;
	}


	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this._collector = arg2;
		try {
			init(fileList);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("id","ts","text"));
		
	}

}

