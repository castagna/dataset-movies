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

package com.kasabi.data.movies;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.openjena.atlas.io.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.LocatorURL;
import com.hp.hpl.jena.util.TypedStream;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class MoviesCommon {

	public static final String DBPEDIA_NS = "http://dbpedia.org/resource/";

	public static final String RDF_FREEBASE_NS = "http://rdf.freebase.com/rdf";

	public static final String KASABI_MOVIES_BASE = "http://data.kasabi.com/dataset/movies/";
	public static final String KASABI_MOVIES_NS = KASABI_MOVIES_BASE ;
	public static final String KASABI_ACTORS_NS = KASABI_MOVIES_BASE + "actors/" ;
	
	public static Model createModel() {
		Model model = ModelFactory.createDefaultModel();
		setPrefixes(model);
		return model;
	}
	
	public static Model createModel(Location location) {
		Model model = TDBFactory.createDataset(location).getDefaultModel();
		setPrefixes(model);
		return model;
	}
	
	private static void setPrefixes(Model model) {
		model.setNsPrefix("rdf", RDF.getURI());
		model.setNsPrefix("rdfs", RDFS.getURI());
		model.setNsPrefix("owl", OWL.getURI());
		model.setNsPrefix("foaf", FOAF.getURI());
		model.setNsPrefix("dc", DC.getURI());
		model.setNsPrefix("dct", DCTerms.getURI());
		model.setNsPrefix("freebase", "http://rdf.freebase.com/ns/");
		model.setNsPrefix("wikipedia", "http://en.wikipedia.org/wiki/");
	}
	
	public static FileManager getFileManager(String namespace) {
		FileManager fm = FileManager.get();
		fm.setModelCaching(false);
		fm.remove(new LocatorURL());
		fm.addLocator(new LocatorCacheURL(namespace));
		return fm;
	}
}

class LocatorCacheURL extends LocatorURL {
	
	private static final Logger log = LoggerFactory.getLogger(LocatorCacheURL.class);
	
	private Cache cache;
	
	public LocatorCacheURL ( String base ) {
		super();
		this.cache = new Cache(base, new File("data/cache"));
		log.debug("LocatorCacheURL({})", base);
	}

    @Override
    public TypedStream open(String url) {
		log.debug("open({})", url);
    	if ( cache.has(url) ) {
    		return new TypedStream(cache.open(url));
    	} else {
    		TypedStream ts = super.open(url);
    		String charset = ts.getCharset();
    		String mime = ts.getMimeType();
    		InputStream in = null;
    		try {
				String content = IO.readWholeFileAsUTF8(ts.getInput());
				cache.put(url, content);
				return new TypedStream( new ByteArrayInputStream(content.getBytes()), mime, charset);
			} catch (IOException e) {
				log.error(e.getMessage());
	    		return null;
			} finally {
				if ( in != null ) try { in.close(); } catch (IOException e) {}
			}
    	}
    }
	
    @Override
    public boolean equals( Object other ) {
        return other instanceof LocatorCacheURL;
    }

    @Override
    public int hashCode() {
        return LocatorCacheURL.class.hashCode();
    }
    
    @Override
    public String getName() { 
    	return "LocatorCacheURL"; 
    } 
	
}
