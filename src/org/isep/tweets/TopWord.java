package org.isep.tweets;

import java.io.Serializable;


public class TopWord implements Comparable<TopWord>, Serializable {
	/**
	 * Generated id
	 */
	private static final long serialVersionUID = -4353992934775333643L;
	public String word;
	public int count;
	@Override
	public int compareTo(TopWord o) {
		return o.count >= this.count ? 1 : -1;
	}
	
}
