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

package com.kasabi.data.movies.imdb;

import java.io.OutputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
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

public class IMDBMovieScraper implements Scraper {

	private String base;
	private OutputStream out;
	private final String LINKEDMDB_SCHEMA_NS = MoviesCommon.LINKEDMDB_NS;
	private final String KASABI_SCHEMA_NS = MoviesCommon.KASABI_MOVIES_SCHEMA;
	
	public IMDBMovieScraper ( String base, OutputStream out ) {
		this.base = base;
		this.out = out;
	}

	@Override
	public Model scrape(Element element) {
		Model model = MoviesCommon.createModel();
		
		String href = element.select("td.title a[href^=/title/]").attr("href");
		String year = element.select("td.title span.year_type").first().text().replaceAll("\\(", "").replaceAll("\\)", "").substring(0,4);
		String title = element.select("td.title a[href^=/title/]").first().text();
		
		if ( ( title != null ) && ( title.length() > 0 ) ) {
			Resource subject = ResourceFactory.createResource(MoviesCommon.KASABI_MOVIES_NS + getMovieSlug(title, year));
			model.add ( subject, RDF.type, ResourceFactory.createResource(LINKEDMDB_SCHEMA_NS + "Film") );
			model.add ( subject, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
			model.add ( subject, DCTerms.title, title );
			if ( year != null ) model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "initial_release_date"), year, new BaseDatatype("http://www.w3.org/2001/XMLSchema#year") );

			Double rating = getRating(element);
			if ( rating != null ) model.add ( model.createLiteralStatement( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "imdbRatingAvg"), rating ) );
			
			Integer votes = getVotes(element);
			if ( votes != null ) model.add ( model.createLiteralStatement( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "imdbRatingVotes"), votes ) );
			
			String certificate = getCertificate(element);
			if ( certificate != null ) model.add ( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "mpaaCertificate"), certificate );
			
			String runtime = getRuntime(element);
			if ( runtime != null ) model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "runtime"), runtime, new BaseDatatype("http://www.w3.org/2001/XMLSchema#duration") );

			scrapeDirectors ( element, subject, model );
			scrapeActors ( element, subject, model );
			scrapeGenres ( element, subject, model );
			
			model.write (System.out, "TURTLE");
			
			model.write ( out, "N-TRIPLES" );			
		}

		return model;
	}
	
	private String getMovieSlug(String title, String year) {
		return Utils.toSlug( title + "-" + year );
	}

	private void scrapeActors ( Element el, Resource subject, Model model ) {
		Elements elements = el.select("span.credit");
		for (Element element : elements) {
			String str = element.toString();
			int indexWith = str.indexOf("With: ");
			if ( indexWith > 0 ) {
				Document doc = Jsoup.parse(str.substring(indexWith).replaceAll("</span>", ""));
				Elements elements2 = doc.select("a");
				for (Element element2 : elements2) {
					String href = element2.attr("href");
					String name = element2.text();

					Resource actor = ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + Utils.toSlug(name) );
					model.add ( actor, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
					model.add ( actor, RDF.type, FOAF.Person );
					model.add ( actor, RDF.type, ResourceFactory.createResource(KASABI_SCHEMA_NS + "Actor") );
					model.add ( actor, FOAF.name, name );

					model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "actor"), actor );
					model.add ( actor,  ResourceFactory.createProperty(KASABI_SCHEMA_NS, "featured_in"), subject );
				}
			}
	
		}
	}

	private void scrapeDirectors ( Element el, Resource subject, Model model ) {
		Elements elements = el.select("span.credit");
		for (Element element : elements) {
			String str = element.toString();
			int indexDir = str.indexOf("Dir: ");
			int indexWith = str.indexOf("With: ");
			if ( ( indexDir > 0 ) && ( indexWith > 0 ) ) {
				Document doc = Jsoup.parse(str.substring(indexDir + 4, indexWith));
				Elements elements2 = doc.select("a");
				for (Element element2 : elements2) {
					String href = element2.attr("href");
					String name = element2.text();

					Resource director = ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + Utils.toSlug(name) );
					model.add ( director, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
					model.add ( director, RDF.type, FOAF.Person );
					model.add ( director, RDF.type, ResourceFactory.createResource(KASABI_SCHEMA_NS + "Director") );
					model.add ( director, FOAF.name, name );

					model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "director"), director );
					model.add ( director,  FOAF.made, subject );
				}
			}
	
		}
	}

	private void scrapeGenres ( Element el, Resource subject, Model model ) {
		Elements elements = el.select("span.genre a");
		for (Element element : elements) {
			String href = element.attr("href");
			model.add ( subject, ResourceFactory.createProperty(KASABI_SCHEMA_NS, "genre"), ResourceFactory.createResource(KASABI_SCHEMA_NS + href.substring(1).replaceAll("_", "-")) );
		}
	}
	
	private Double getRating(Element element) {
		Double rating = null;
		Elements elements = element.select("span.rating-rating span.value");
		if ( !elements.isEmpty() ) {
			try {
				rating = Double.parseDouble( elements.first().text().trim() );
			} catch ( NumberFormatException e ) {}
		}
		return rating;
	}
	
	private String getRuntime(Element element) {
		String runtime = null;
		Elements elements = element.select("span.runtime");
		if ( !elements.isEmpty() ) {
			try {
				int minutes = Integer.parseInt( elements.first().text().replaceAll(" mins\\.", "").trim() );
				runtime = "PT" + ( minutes / 60 ) + "H" + ( minutes % 60 ) + "M";
			} catch ( NumberFormatException e ) {}
		}
		return runtime;
	}
	
	
	private Integer getVotes(Element el) {
		Integer votes = null;
		Elements elements = el.select("div[class=rating rating-list]");
		if ( !elements.isEmpty() ) {
			String str = elements.first().attr("title").trim();
			int indexStart = str.indexOf('(');
			if ( indexStart > 0 ) {
				str = str.substring(indexStart + 1);
				int indexEnd = str.indexOf(" votes)");
				if ( indexEnd > 0 ) {
					str = str.substring(0, indexEnd);
					str = str.replaceAll(",", "");
					try {
						votes = Integer.parseInt( str.trim() );
					} catch ( NumberFormatException e ) {}
				}
			}
		}
		return votes;
	}
	
	private String getCertificate(Element el) {
		String certificate = null;
		Elements elements = el.select("span.certificate img");
		if ( !elements.isEmpty() ) {
			certificate = elements.first().attr("title").trim().replaceAll("_", "-");
		}
		return certificate;
	}
	
}
