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

package com.kasabi.data.movies.dbpedia;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.kasabi.data.movies.BaseHarvester;
import com.kasabi.data.movies.Harvester;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.labs.datasets.Utils;

public class DBPediaRDFHarvester extends BaseHarvester implements Harvester {

	public static final Logger log = LoggerFactory.getLogger(DBPediaRDFHarvester.class);
	
	public DBPediaRDFHarvester ( String base, String filename, String charset, String type, Resource new_type, OutputStream out ) {
		super ( base, filename, charset, type, new_type, out );
	}

	@Override
	public void harvest() {
		try {
			InputStream is = new FileInputStream (filename);
			Reader reader = charset != null ? new InputStreamReader ( is, charset ) : new InputStreamReader ( is );
			BufferedReader in = new BufferedReader ( reader );
			String line;
			while ( ( line = in.readLine() ) != null ) {
				String[] tokens = line.split("\t");
				if ( tokens.length == 2 ) {
					String name = tokens[0];
					String url = tokens[1];
					Model model = load ( fm.openNoMap(url), MoviesCommon.DBPEDIA_NS );
					
					Resource subject = ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + Utils.toSlug(name));
					Resource other_subject = ResourceFactory.createResource(url);
					QuerySolutionMap qsm = new QuerySolutionMap();
					qsm.add("subject", subject);
					qsm.add("label", model.createLiteral(name));
					qsm.add("type", new_type);
					qsm.add("dbpedia_subject", other_subject);
					
					Model result = MoviesCommon.createModel();
					result.add ( getModel("src/main/resources/dbpedia", model, qsm) );
					result.add ( getModel("src/main/resources/dbpedia" + type, model, qsm) );

					model.write(System.out, "TURTLE");
					result.write(System.out, "TURTLE");

					model.write(out, "N-TRIPLES");
					result.write(out, "N-TRIPLES");
				}
			}			
		} catch ( IOException e ) {
			log.error(e.getMessage(), e);
		}
	}
	
}
