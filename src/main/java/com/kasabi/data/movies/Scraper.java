package com.kasabi.data.movies;

import org.jsoup.nodes.Element;

import com.hp.hpl.jena.rdf.model.Model;

public interface Scraper {

	public Model scrape(Element element);
	
}
