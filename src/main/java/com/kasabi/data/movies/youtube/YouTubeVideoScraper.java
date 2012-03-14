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

package com.kasabi.data.movies.youtube;

import java.io.OutputStream;

import org.jsoup.nodes.Element;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.Scraper;
import com.kasabi.labs.datasets.Utils;

public class YouTubeVideoScraper implements Scraper {

	private String base;
	private OutputStream out;
	private final String LINKEDMDB_SCHEMA_NS = MoviesCommon.LINKEDMDB_NS;
	
	public YouTubeVideoScraper ( String base, OutputStream out ) {
		this.base = base;
		this.out = out;
	}

	@Override
	public Model scrape(Element element) {
		Model model = MoviesCommon.createModel();
		
		String href = element.select("a[href^=/watch]").attr("href");
		href = href.substring(0, href.indexOf('&'));
		String title = element.attr("title");
		int index = title.indexOf('(');
		String year = null;
		if ( index > 0 ) {
			year = title.substring(index + 1, index + 5);
			title = title.substring ( 0, index ).trim();
		}

		if ( ( title != null ) && ( title.length() > 0 ) ) {
			Resource subject = ResourceFactory.createResource(MoviesCommon.KASABI_MOVIES_NS + getMovieSlug(title, year));
			model.add ( subject, RDF.type, ResourceFactory.createResource(MoviesCommon.BIBO_NS + "Film") );
			model.add ( subject, FOAF.isPrimaryTopicOf, ResourceFactory.createResource(base + href) );
			model.add ( subject, ResourceFactory.createProperty(MoviesCommon.SCHEMA_ORG_NS, "video"), ResourceFactory.createResource((base + href).replaceAll("watch\\?v=", "embed/")) );
			model.add ( subject, RDFS.label, title + "(" + year + ")", "en-gb" );
			model.add ( subject, DCTerms.title, title, "en-gb" );
			if ( year != null ) model.add ( subject, ResourceFactory.createProperty(LINKEDMDB_SCHEMA_NS, "initial_release_date"), year, new BaseDatatype("http://www.w3.org/2001/XMLSchema#year") );

			model.write (System.out, "TURTLE");
			
			model.write ( out, "N-TRIPLES" );			
		}
		
		return model;
	}
	
	private String getMovieSlug(String title, String year) {
		return Utils.toSlug( title + "-" + year );
	}
	
}
