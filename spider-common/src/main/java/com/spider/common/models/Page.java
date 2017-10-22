package com.spider.common.models;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {
	@JsonProperty
	URI location;

	@JsonProperty
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm a z")
	Date lastVisit;

	@JsonProperty
	String htmlRaw;
	
	@JsonProperty
	List<String> links;

	public Page() {
	}

	public Page(Builder builder) {
		location = builder.location;
		lastVisit = builder.lastVisit;
	}

	public static class Builder {
		URI location;
		Date lastVisit;

		public Builder location(String uri) throws URISyntaxException {
			location(new URI(uri));
			return this;
		}
		
		public Builder location(URI uri) {
			this.location = uri;
			return this;
		}

		public Builder lastVisit(Date date) {
			this.lastVisit = date;
			return this;
		}

		public Page build() {
			return new Page(this);
		}
	}

	@Override
	public String toString() {
		return "URL: " + location +
		" Last visit: " + lastVisit +
		" HTML: " + ((htmlRaw != null && !htmlRaw.isEmpty())? htmlRaw.substring(0, 60).replaceAll("[\\t\\n\\r ]+", "") + "..." : "null");
	}

	public URI getLocation() {
		return location;
	}

	public Date getLastVisit() {
		return lastVisit;
	}

	public String getHtmlRaw() {
		return htmlRaw;
	}

	public void setHtmlRaw(String htmlRaw) {
		this.lastVisit = new Date();
		this.htmlRaw = htmlRaw;
	}

	public List<String> getLinks() {
		return links;
	}

	public void setLinks(List<String> links) {
		this.links = links;
	}
}
