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

import static com.freebase.json.JSON.a;
import static com.freebase.json.JSON.o;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.json.simple.parser.ParseException;
import org.openjena.atlas.io.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.freebase.api.Freebase;
import com.freebase.json.JSON;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.LocatorURL;
import com.hp.hpl.jena.util.TypedStream;
import com.kasabi.data.movies.Cache;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.labs.datasets.Utils;

public class FreebaseMovies {
	
	public static final Logger log = LoggerFactory.getLogger(FreebaseMovies.class);
	
	public static final String RDF_FREEBASE_NS = "http://rdf.freebase.com/rdf";
	
	public static void main(String[] args) throws ParseException {
//		list ( "/film/actor", MoviesCommon.KASABI_ACTORS_NS, ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + "Actor") );
		list ( "/film/film", MoviesCommon.KASABI_MOVIES_NS, ResourceFactory.createResource(MoviesCommon.KASABI_MOVIES_NS + "Movie") );
	}
	
	private static void list( String type, String namespace, Resource new_type ) {
		// See: http://mql.freebaseapps.com/ch03.html
		Freebase freebase = Freebase.getFreebase();
		JSON query = a(o(
			"id", null,
			"type", type,
			"name", null
		));
		Object cursor = true;
		int count = 0;
		
		FileManager fm = getFileManager(); 
		while ( !cursor.equals(false) ) {
			JSON envelope = o( 
				"cursor", cursor 
			);
			JSON response = freebase.mqlread(query, envelope, null);
			@SuppressWarnings("unchecked")
			List topics = response.get("result").array();
			log.debug ("Found {] topics...", topics.size());
			for ( Object topic : topics ) {
				String topic_id = ((JSON)topic).get("id").string();
				String topic_name = ((JSON)topic).get("name").string();	
				String topic_rdf_url = RDF_FREEBASE_NS + topic_id;
				Model model = load ( fm.openNoMap(topic_rdf_url), RDF_FREEBASE_NS );

				if ( topic_name != null ) {
					Resource new_subject = ResourceFactory.createResource(namespace + Utils.toSlug(topic_name));
					Resource freebase_subject = ResourceFactory.createResource(topic_rdf_url);
					QuerySolutionMap qsm = new QuerySolutionMap();
					qsm.add("subject", new_subject);
					qsm.add("type", new_type);
					qsm.add("freebase_subject", freebase_subject);

					Model result = MoviesCommon.createModel();
					result.add ( getModel("src/main/resources/freebase", model, qsm) );
					result.add ( getModel("src/main/resources/freebase" + type, model, qsm) );

//					model.write(System.out, "TURTLE");
//					System.out.println("=============================");
					result.write(System.out, "TURTLE");

					log.debug("{} - {} ({}) retrieved {} triples", new Object[]{++count, topic_name, topic_id, model.size()});
				} else {
					log.warn("Topic {} has no label...", topic_id);
				}
			};
			cursor = response.get("cursor");
		}
	}

	private static Model load ( InputStream in, String base ) {
		Model model = MoviesCommon.createModel();
		return model.read(in, base);
	}
	
	private static FileManager getFileManager() {
		FileManager fm = FileManager.get();
		fm.setModelCaching(false);
		fm.remove(new LocatorURL());
		fm.addLocator(new LocatorCacheURL(RDF_FREEBASE_NS));
		return fm;
	}
	
	public static Model getModel(String pathname, Model model, QuerySolutionMap qsm) {
		Model result = MoviesCommon.createModel();
		
		File path = new File(pathname);
		File[] files = path.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".rq");
			}
		});
		Arrays.sort(files);

		for (File file : files) {
			log.debug("Executing {} ...", file.getName());
			Query query = QueryFactory.read(file.getAbsolutePath());
			QueryExecution qexec = QueryExecutionFactory.create(query, model);
			qexec.setInitialBinding(qsm);
			try {
				result.add ( qexec.execConstruct() );
			} finally {
				qexec.close();
			}
		}
		return result;
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