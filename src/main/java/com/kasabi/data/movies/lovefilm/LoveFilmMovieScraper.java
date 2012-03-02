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

import java.io.OutputStream;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.Scraper;
import com.kasabi.labs.datasets.Utils;

public class LoveFilmMovieScraper implements Scraper {

	private String base;
	private OutputStream out;
	private final String LINKEDMDB_SCHEMA_NS = MoviesCommon.LINKEDMDB_NS;
	private final String KASABI_SCHEMA_NS = MoviesCommon.KASABI_MOVIES_SCHEMA;
	
	public LoveFilmMovieScraper ( String base, OutputStream out ) {
		this.base = base;
		this.out = out;
	}

	@Override
	public Model scrape(Element element) {
		Model model = MoviesCommon.createModel();
		
		String href = element.select("h2 > a[href^=http://www.lovefilm.com/film/]").first().attr("href").trim();
		String year = getYear(element);

		Resource subject = ResourceFactory.createResource(MoviesCommon.KASABI_MOVIES_NS + getMovieSlug(href, year));
		model.add ( subject, RDF.type, ResourceFactory.createResource(LINKEDMDB_SCHEMA_NS + "Film") );
		model.add ( subject, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(href) );
		
		String title = getTitle(element);
		if ( title != null ) {
			model.add ( subject, DCTerms.title, title );
		}
		
		if ( year != null ) model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "initial_release_date"), year, new BaseDatatype("http://www.w3.org/2001/XMLSchema#year") );

		String certificate = getCertificate(element);
		if ( certificate != null ) model.add ( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "bbfcCertificate"), certificate );

		Double rating = getRating(element);
		if ( rating != null ) model.add ( model.createLiteralStatement( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "lovefilmRatingAvg"), rating ) );
		
		Integer votes = getVotes(element);
		if ( votes != null ) model.add ( model.createLiteralStatement( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "lovefilmRatingVotes"), votes ) );

		String amazonPage = getAmazonPage(element);
		if ( amazonPage != null ) model.add ( subject, FOAF.page, ResourceFactory.createResource(amazonPage) );
		
		scrapeActors ( element, subject, model );
		scrapeDirectors ( element, subject, model );
		
		model.write ( out, "N-TRIPLES" );
		return model;
	}
	
	private String getMovieSlug(String href, String year) {
		String slug = href.replaceAll("http://www.lovefilm.com/film/", "");
		slug = slug.substring(0, slug.indexOf('/') );
		if ( year != null ) slug = slug + "-" + year;
		return Utils.toSlug( slug );
	}

	private String getPersonSlug(String href) {
		String slug = href.substring( href.lastIndexOf('/') ).replaceAll(".html", "").replaceAll("_", "-");
		return Utils.toSlug( slug );
	}

	private void scrapeActors ( Element el, Resource subject, Model model ) {
		Elements elements = el.select("div:matches(Starring) > a");
		for (Element element : elements) {
			String href = element.attr("href");
			String name = element.text();

			Resource actor = ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + getPersonSlug(href) );
			model.add ( actor, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
			model.add ( actor, RDF.type, FOAF.Person );
			model.add ( actor, RDF.type, ResourceFactory.createResource(KASABI_SCHEMA_NS + "Actor") );
			model.add ( actor, FOAF.name, name );

			model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "actor"), actor );
			model.add ( actor,  ResourceFactory.createProperty(KASABI_SCHEMA_NS, "featured_in"), subject );
		}
	}
	
	private void scrapeDirectors ( Element el, Resource subject, Model model ) {
		Elements elements = el.select("div:matches(Director) > a");
		for (Element element : elements) {
			String href = element.attr("href");
			String name = element.text();

			Resource director = ResourceFactory.createResource(MoviesCommon.KASABI_DIRECTORS_NS + getPersonSlug(href) );
			model.add ( director, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
			model.add ( director, RDF.type, FOAF.Person );
			model.add ( director, RDF.type, ResourceFactory.createResource(KASABI_SCHEMA_NS + "Director") );
			model.add ( director, FOAF.name, name );

			model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "director"), director );
		}
	}
	
	private String getTitle(Element el) {
		String title = null;
		Elements elements = el.select("h2 > a[href^=http://www.lovefilm.com/film/]");
		if ( !elements.isEmpty() ) {
			title = elements.first().text().trim();
			title = title.replaceAll(" - Blu-ray", "");
		}
		return title;
	}

	private String getYear(Element el) {
		String year = null;
		Elements elements = el.select("span.release_decade");
		if ( !elements.isEmpty() ) {
			year = elements.first().text().trim().replaceAll("\\(", "").replaceAll("\\)", "");
		}
		return year;
	}
	
	private String getCertificate(Element el) {
		String certificate = null;
		Elements elements = el.select("div.certif img");
		if ( !elements.isEmpty() ) {
			certificate = elements.first().attr("alt").trim();
		}
		return certificate;
	}
	
	private Double getRating(Element el) {
		Double rating = null;
		Elements elements = el.select("li[property=v:rating]");
		if ( !elements.isEmpty() ) {
			rating = Double.parseDouble( elements.first().attr("content").trim() );
		}
		return rating;
	}
	
	private Integer getVotes(Element el) {
		Integer votes = null;
		Elements elements = el.select("small[property=v:count]");
		if ( !elements.isEmpty() ) {
			votes = Integer.parseInt( elements.first().attr("content").trim() );
		}
		return votes;
	}
	
	private String getAmazonPage(Element el) {
		String url = null;
		Elements elements = el.previousElementSibling().select("a.btn_buy");
		if ( !elements.isEmpty() ) {
			String href = elements.first().attr("href").trim(); 
			url = href.substring(0, href.lastIndexOf('/'));
		}
		return url;
	}

}
