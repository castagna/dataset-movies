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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;

public class LoveFilmLinking {

	public static void main(String[] args) throws IOException {
		Location location = new Location ("/tmp/lovefilm");
		Dataset dataset = TDBFactory.createDataset(location);
		Model model = dataset.getDefaultModel();
		
		link_actors ( model );
		link_directors ( model );

	}
	
	private static void link_actors ( Model model ) throws IOException {
		BufferedReader in = new BufferedReader ( new InputStreamReader ( new FileInputStream ("/opt/datasets/movies/actors.txt") ) );
		PrintWriter out = new PrintWriter ( "/opt/datasets/movies/actors-links-lovefilm.tsv" );
		String line;
		while ( (line = in.readLine()) != null ) {
			ResIterator iter = model.listSubjectsWithProperty(FOAF.name, line);
			if ( !iter.hasNext() ) {
				out.println(line);
			} else {
				while ( iter.hasNext() ) {
					Resource resource = iter.next();
					out.println(line + "\t" + resource.getURI());
				}				
			}
		}
		out.close();
	}
	
	private static void link_directors ( Model model ) throws IOException {
		BufferedReader in = new BufferedReader ( new InputStreamReader ( new FileInputStream ("/opt/datasets/movies/directors.txt") ) );
		PrintWriter out = new PrintWriter ( "/opt/datasets/movies/directors-links-lovefilm.tsv" );
		String line;
		while ( (line = in.readLine()) != null ) {
			ResIterator iter = model.listSubjectsWithProperty(FOAF.name, line);
			if ( !iter.hasNext() ) {
				out.println(line);
			} else {
				while ( iter.hasNext() ) {
					Resource resource = iter.next();
					out.println(line + "\t" + resource.getURI());
				}
			}			
		}
		out.close();
	}

}
