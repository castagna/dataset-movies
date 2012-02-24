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

package com.kasabi.data.movies.freebase;

import static com.kasabi.data.movies.MoviesCommon.RDF_FREEBASE_NS;

import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freebase.api.Freebase;
import com.freebase.json.JSON;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.OWL;
import com.kasabi.data.movies.MoviesCommon;

public class FreebaseReconciliation {

	protected static final Logger log = LoggerFactory.getLogger(FreebaseReconciliation.class) ;
	private static final FileManager fm = MoviesCommon.getFileManager(RDF_FREEBASE_NS); 
	
	public static void main(String[] args) {
		Freebase freebase = Freebase.getFreebase();
		get(freebase, "Ingrid Thulin").write(System.out, "TURTLE");
		
//		JSON response = freebase.search("Ingrid Thulin");
//		JSON response = freebase.search("Ocean's Eleven");
		JSON response = freebase.search("Ocean's Eleven 2001"); // title and year so that we can disambiguate movies with same title (assuming they are remakes and done in different years) 
		@SuppressWarnings("unchecked")
		List topics = response.get("result").array();
		for ( Object topic : topics ) {
			JSON json = (JSON)topic;
			System.out.println(json.get("id").string() + " " + json.get("name").string() + " (" + json.get("relevance:score") + ")");
		}
	}
	
	public static String getURI(Freebase freebase, String string) {
		String uri = null;
		JSON response = freebase.search(string); 
		@SuppressWarnings("unchecked")
		List topics = response.get("result").array();
		for ( Object topic : topics ) {
			JSON json = (JSON)topic;
			uri = RDF_FREEBASE_NS + json.get("id").string();
		}
		log.debug("getURI({}, {}) --> {}", new Object[]{freebase, string, uri});
		return uri;
	}
	
	public static Model get(Freebase freebase, String string) {
		Model model = MoviesCommon.createModel();
		String uri = getURI(freebase, string);
		if ( uri != null ) {
			InputStream in = fm.openNoMap(uri);
			model.read(in, RDF_FREEBASE_NS);			
		}
		log.debug("get({}, {}) --> {} triples", new Object[]{freebase, string, model.size()});
		return model;
	}
	
	public static Model get( Freebase freebase, String string, Resource subject ) {
		Model model = MoviesCommon.createModel();
		String uri = getURI(freebase, string);
		if ( uri != null ) {
			InputStream in = fm.openNoMap(uri);
			model.read ( in, RDF_FREEBASE_NS );
			Resource r = ResourceFactory.createResource(uri);
			model.add ( subject, OWL.sameAs, r );
			model.add ( r, OWL.sameAs, subject );
		}
		log.debug("get({}, {}, {}) --> {} triples", new Object[]{freebase, string, subject, model.size()});
		return model;
	}

}
