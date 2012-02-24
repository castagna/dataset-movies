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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.kasabi.data.movies.MoviesCommon;

// TODO: this is not going to work...
// "Lines are grouped by <source> and <property> and are ordered by a sort 
//  index when available, meaning all assertions about a particular topic 
//  with a particular relationship are contiguous and sorted roughly by importance." 
//  -- http://wiki.freebase.com/wiki/Data_dumps

public class FreebaseMovies2RDF {

	public static final Logger log = LoggerFactory.getLogger(FreebaseMovies2RDF.class);
	public static final String FREEBASE_NS = "http://rdf.freebase.com/ns";
	public static final String LANG = "/lang/";
	
	public static void main(String[] args) throws Exception {
		String filename = "/backups/tmp/freebase-datadump-quadruples.tsv.bz2";
	    BufferedReader in = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(filename))));
	    String line;
	    int count = 0;
	    
	    Model model = MoviesCommon.createModel();
	    String prev_subject = null;
	    while ( ( line = in.readLine() ) != null ) {
	    	count++;
	    	String[] tokens = line.split("\\t");
	    	
	    	if ( tokens.length > 0 ) {
	    		String subject = tokens[0].trim();
	    		if ( !subject.equals(prev_subject) ) {
	    			process ( model );
	    			model = MoviesCommon.createModel();
	    		}
	    		prev_subject = subject;
		    	if ( ( tokens.length == 3 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[2].trim().length() > 0) ) {    		
		    		output_resource ( model, tokens[0], tokens[1], tokens[2] );
		    	} else if ( ( tokens.length == 4 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[3].trim().length() > 0) ) {
		    		if ( tokens[2].trim().length() == 0 ) {
		    			output_literal ( model, tokens[0], tokens[1], tokens[3] );
		    		} else {
		    			if ( tokens[2].startsWith(LANG) ) {
		    				output_literal_lang ( model, tokens[0], tokens[1], tokens[3], tokens[2] );
		    			} else {
		    				if ( tokens[1].equals("/type/object/key") ) {
		    					output_literal2 ( model, tokens[0], tokens[1], tokens[2], tokens[3] );
		    				} else if ( (tokens[1].equals("/type/object/name")) && (tokens[2].startsWith("/guid/")) ) {
		    					output_literal2 ( model, tokens[0], tokens[1], tokens[2], tokens[3] );
		    				} else {
		    					log.warn ("Unexpected data at {}, ignoring: {}", count, line);
		    				}
		    			}
		    		}    		
		    	} else {
		    		if ( tokens.length < 3 ) { 
		    			log.warn ("Line {} has only {} tokens: {}", new Object[]{count, tokens.length, line});
		    		} else {
		    			log.warn ("Line {} has one or more empty tokens: {}", new Object[]{count, line});
		    		}
		    	}
	    	
	    	}
    		if ( count % 1000000 == 0 ) log.info("Processed {} lines...", count);
	    }
	}
	
	private static void process(Model model) {
		if ( model.containsResource(ResourceFactory.createResource(FREEBASE_NS + "/film.film")) ) {
			model.write(System.out, "TURTLE");			
		}
	}

	private static void output_resource ( Model model, String subject, String predicate, String object ) throws IOException {
		model.add ( ResourceFactory.createResource(FREEBASE_NS + subject), ResourceFactory.createProperty(FREEBASE_NS + predicate), ResourceFactory.createResource(FREEBASE_NS + object) );
	}

	private static void output_literal ( Model model, String subject, String predicate, String literal ) throws IOException {
		model.add ( ResourceFactory.createResource(FREEBASE_NS + subject), ResourceFactory.createProperty(FREEBASE_NS + predicate), literal );
	}

	private static void output_literal2 ( Model model, String subject, String predicate1, String predicate2, String literal ) throws IOException {
		model.add ( ResourceFactory.createResource(FREEBASE_NS + subject), ResourceFactory.createProperty(FREEBASE_NS + predicate1 ), predicate2 + ":" + literal );
	}

	private static void output_literal_lang ( Model model, String subject, String predicate, String literal, String lang ) throws IOException {
		model.add ( ResourceFactory.createResource(FREEBASE_NS + subject), ResourceFactory.createProperty(FREEBASE_NS + predicate), literal, lang );
	}
	
}
