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

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.json.simple.parser.ParseException;
import org.openjena.atlas.lib.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freebase.api.Freebase;
import com.freebase.json.JSON;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.OWL;
import com.kasabi.data.movies.BaseLinker2;
import com.kasabi.data.movies.Cache;
import com.kasabi.data.movies.Linker2;
import com.kasabi.data.movies.MoviesCommon;

public abstract class FreebaseBaseLinker2 extends BaseLinker2 implements Linker2 {

	private static final Logger log = LoggerFactory.getLogger(FreebaseBaseLinker2.class);
	
	private final FileManager fm;
	protected final Freebase freebase;
	protected final Cache cache;
	
	public FreebaseBaseLinker2(String base) {
		super(base);
		this.fm = MoviesCommon.getFileManager(base); 
		this.freebase = Freebase.getFreebase();
		this.cache = new Cache(base, new File("data/cache"));
	}

	protected String getURI(String string) {
		String uri = null;
		
		JSON response = null;
		if ( cache.has(string) ) {
			try {
				response = JSON.parse(cache.get(string));
			} catch (ParseException e) {
				log.error (e.getMessage(), e);
			}
		} else {
			response = freebase.search(string); 
			cache.put(string, response.toJSONString());
		}
		
		@SuppressWarnings({ "rawtypes" })
		List topics = response.get("result").array();
		if ( topics.size() > 0 ) {
			JSON json = (JSON)topics.get(0);
			uri = RDF_FREEBASE_NS + json.get("id").string();
		}

		log.debug("getURI({}, {}) --> {}", new Object[]{freebase, string, uri});
		return uri;
	}

	protected Pair<String,Model> get(String string) {
		Model model = MoviesCommon.createModel();
		String uri = getURI(string);
		if ( uri != null ) {
			InputStream in = fm.openNoMap(uri);
			model.read(in, RDF_FREEBASE_NS);			
		}
		log.debug("get({}, {}) --> {}, {} triples", new Object[]{freebase, string, uri, model.size()});
		return new Pair<String,Model>(uri, model);
	}

	protected Model get( String string, Resource subject ) {
		Model model = MoviesCommon.createModel();
		String uri = getURI(string);
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
