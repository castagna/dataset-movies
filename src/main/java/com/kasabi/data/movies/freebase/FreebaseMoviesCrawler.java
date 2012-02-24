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
import static com.kasabi.data.movies.MoviesCommon.RDF_FREEBASE_NS;

import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.parser.ParseException;
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
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.labs.datasets.Utils;

public class FreebaseMoviesCrawler {
	
	public static final Logger log = LoggerFactory.getLogger(FreebaseMoviesCrawler.class);
	public static final ExecutorService executor = Executors.newFixedThreadPool(100) ;
	private static final FileManager fm = MoviesCommon.getFileManager(RDF_FREEBASE_NS); 
	
	public static void main(String[] args) throws ParseException {
//		list ( "/film/actor", MoviesCommon.KASABI_ACTORS_NS, ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + "Actor") );
		list ( "/film/film", MoviesCommon.KASABI_MOVIES_NS, ResourceFactory.createResource(MoviesCommon.KASABI_MOVIES_NS + "Movie") );
	}
	
	private static void list( final String type, final String namespace, final Resource new_type ) {
		// See: http://mql.freebaseapps.com/ch03.html
		Freebase freebase = Freebase.getFreebase();
		JSON query = a(o(
			"id", null,
			"type", type,
			"name", null
		));
		Object cursor = true;
		int count = 0;
		
		while ( !cursor.equals(false) ) {
			JSON envelope = o( 
				"cursor", cursor 
			);
			JSON response = freebase.mqlread(query, envelope, null);
			@SuppressWarnings("unchecked")
			List topics = response.get("result").array();
			log.debug ("Found {} topics...", topics.size());
			for ( Object topic : topics ) {
				count++;
				log.debug ("Retrieving topic {} ...", count);
				final String topic_id = ((JSON)topic).get("id").string();
				final String topic_name = ((JSON)topic).get("name").string();	
				final String topic_rdf_url = RDF_FREEBASE_NS + topic_id;
		        try {
		            final Callable<Boolean> task = new Callable<Boolean>() {
		                @Override
		                public Boolean call() throws Exception {
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

//		    					model.write(System.out, "TURTLE");
//		    					System.out.println("=============================");
//		    					result.write(System.out, "TURTLE");

		    					log.debug("{} ({}) retrieved {} triples", new Object[]{topic_name, topic_id, model.size()});
		    				} else {
		    					log.warn("Topic {} has no label...", topic_id);
		    					return Boolean.FALSE ;
		    				}
		                    return Boolean.TRUE ;
		            }} ;
		            executor.submit(task) ;
		        } catch (RuntimeException e) {
		            log.error(e.getMessage()) ;
		        }
			};
			cursor = response.get("cursor");
		}
	}

	private static Model load ( InputStream in, String base ) {
		Model model = MoviesCommon.createModel();
		return model.read(in, base);
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

