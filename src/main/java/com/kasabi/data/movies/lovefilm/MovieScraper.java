/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kasabi.data.movies.lovefilm;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.freebase.api.Freebase;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.freebase.FreebaseReconciliation;
import com.kasabi.labs.datasets.Utils;

public class MovieScraper {

	private Element element;
	private String base;
	private final String SCHEMA_NS = MoviesCommon.KASABI_MOVIES_SCHEMA;
	private final Freebase freebase = Freebase.getFreebase();
	private final boolean freebase_reconciliation = false; // too slow!
	
	public MovieScraper ( String base, Element element ) {
		this.base = base;
		this.element = element;
	}

	public Model getModel() {
		Model model = MoviesCommon.createModel();
		
		String href = element.select("h2 > a[href^=http://www.lovefilm.com/film/]").first().attr("href").trim();
		
		Resource subject = ResourceFactory.createResource(MoviesCommon.KASABI_MOVIES_NS + getMovieSlug(href));
		model.add ( subject, RDF.type, ResourceFactory.createResource(SCHEMA_NS + "Movie") );
		model.add ( subject, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(href) );
		
		String title = getTitle();
		if ( title != null ) {
			model.add ( subject, RDFS.label, title );
			if ( freebase_reconciliation ) model.add ( FreebaseReconciliation.get ( freebase, title, subject ) );
		}
		
		String year = getYear();
		if ( year != null ) model.add ( subject, ResourceFactory.createProperty(SCHEMA_NS, "year"), year );

		String certificate = getCertificate();
		if ( certificate != null ) model.add ( subject, ResourceFactory.createProperty(SCHEMA_NS, "bbfcCertificate"), certificate );

		Double rating = getRating();
		if ( rating != null ) model.add ( model.createLiteralStatement( subject, ResourceFactory.createProperty(SCHEMA_NS, "lovefilmRatingAvg"), rating ) );
		
		Integer votes = getVotes();
		if ( votes != null ) model.add ( model.createLiteralStatement( subject, ResourceFactory.createProperty(SCHEMA_NS, "lovefilmRatingVotes"), votes ) );

		String amazonPage = getAmazonPage();
		if ( amazonPage != null ) model.add ( subject, RDFS.seeAlso, ResourceFactory.createResource(amazonPage) );
		
		scrapeActors ( subject, model );
		scrapeDirectors ( subject, model );
		
		return model;
	}
	
	private String getMovieSlug(String href) {
		String slug = href.replaceAll("http://www.lovefilm.com/film/", "");
		slug = slug.substring(0, slug.indexOf('/') );
		return Utils.toSlug( slug );
	}

	private String getPersonSlug(String href) {
		String slug = href.substring( href.lastIndexOf('/') ).replaceAll(".html", "").replaceAll("_", "-");
		return Utils.toSlug( slug );
	}

	private void scrapeActors ( Resource subject, Model model ) {
		Elements elements = element.select("div:matches(Starring) > a");
		for (Element element : elements) {
			String href = element.attr("href");
			String name = element.text();

			Resource actor = ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + getPersonSlug(href) );
			model.add ( actor, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
			model.add ( actor, RDF.type, FOAF.Person );
			model.add ( actor, RDF.type, ResourceFactory.createResource(SCHEMA_NS + "Actor") );
			model.add ( actor, RDFS.label, name );
			model.add ( actor, FOAF.name, name );

			model.add ( subject, ResourceFactory.createProperty(SCHEMA_NS, "actor"), actor );
			
			if ( freebase_reconciliation ) model.add ( FreebaseReconciliation.get ( freebase, name, actor ) );
		}
	}
	
	private void scrapeDirectors ( Resource subject, Model model ) {
		Elements elements = element.select("div:matches(Director) > a");
		for (Element element : elements) {
			String href = element.attr("href");
			String name = element.text();

			Resource director = ResourceFactory.createResource(MoviesCommon.KASABI_DIRECTORS_NS + getPersonSlug(href) );
			model.add ( director, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
			model.add ( director, RDF.type, FOAF.Person );
			model.add ( director, RDF.type, ResourceFactory.createResource(SCHEMA_NS + "Director") );
			model.add ( director, RDFS.label, name );
			model.add ( director, FOAF.name, name );

			model.add ( subject, ResourceFactory.createProperty(SCHEMA_NS, "director"), director );
			
			if ( freebase_reconciliation ) model.add ( FreebaseReconciliation.get ( freebase, name, director ) );
		}
	}
	
	private String getTitle() {
		String title = null;
		Elements elements = element.select("h2 > a[href^=http://www.lovefilm.com/film/]");
		if ( !elements.isEmpty() ) {
			title = elements.first().text().trim();
		}
		return title;
	}

	private String getYear() {
		String year = null;
		Elements elements = element.select("span.release_decade");
		if ( !elements.isEmpty() ) {
			year = elements.first().text().trim().replaceAll("\\(", "").replaceAll("\\)", "");
		}
		return year;
	}
	
	private String getCertificate() {
		String certificate = null;
		Elements elements = element.select("div.certif img");
		if ( !elements.isEmpty() ) {
			certificate = elements.first().attr("alt").trim();
		}
		return certificate;
	}
	
	private Double getRating() {
		Double rating = null;
		Elements elements = element.select("li[property=v:rating]");
		if ( !elements.isEmpty() ) {
			rating = Double.parseDouble( elements.first().attr("content").trim() );
		}
		return rating;
	}
	
	private Integer getVotes() {
		Integer votes = null;
		Elements elements = element.select("small[property=v:count]");
		if ( !elements.isEmpty() ) {
			votes = Integer.parseInt( elements.first().attr("content").trim() );
		}
		return votes;
	}
	
	private String getAmazonPage() {
		String url = null;
		Elements elements = element.previousElementSibling().select("a.btn_buy");
		if ( !elements.isEmpty() ) {
			String href = elements.first().attr("href").trim(); 
			url = href.substring(0, href.lastIndexOf('/'));
		}
		return url;
	}

}
