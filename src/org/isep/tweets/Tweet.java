package org.isep.tweets;


public class Tweet implements Comparable<Tweet> {
	private Long id;
	private Long timestamp;
	private String text;

	public Tweet(Long id, long creation, String text) {
		this.id = id;
		this.timestamp = creation;
		this.text = text;

	}

	public Long getId() {
		return id;
	}

	public String getText() {
		return text;
	}

	public Long getCreationDate() {
		return timestamp;
	}

	@Override
	public int compareTo(Tweet o) {
		if (o.equals(this))
			return 0;
		else
			return timestamp >= o.timestamp ? -1 : 1;
	}

}
