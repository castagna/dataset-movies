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

import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.FileManager;

public abstract class BaseHarvester {

	public static final Logger log = LoggerFactory.getLogger(BaseHarvester.class);
	
	protected String filename;
	protected String charset;
	protected String type;
	protected Resource new_type;
	protected final FileManager fm;
	private final Map<String, Set<Query>> queries = new HashMap<String, Set<Query>>();
	protected OutputStream out;
	
	public BaseHarvester ( String base, String filename, String charset, String type, Resource new_type, OutputStream out ) {
		this.filename = filename;
		this.charset = charset;
		this.fm = MoviesCommon.getFileManager(base); 
		this.type = type;
		this.new_type = new_type;
		this.out = out;
	}
	
	protected Model load ( InputStream in, String base ) {
		Model model = MoviesCommon.createModel();
		return model.read(in, base);
	}
	
	protected Set<Query> getQueries(String pathname) {
		if ( !queries.containsKey(pathname) ) {
			File path = new File(pathname);
			File[] files = path.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".rq");
				}
			});
			Arrays.sort(files);
			Set<Query> qs = new HashSet<Query>();
			for (File file : files) {
				Query query = QueryFactory.read(file.getAbsolutePath());
				qs.add(query);
			}
			queries.put(pathname, qs);
		}
		return queries.get(pathname);
	}
	
	protected Model getModel(String pathname, Model model, QuerySolutionMap qsm) {
		Model result = MoviesCommon.createModel();
		Set<Query> queries = getQueries(pathname);
		for (Query query : queries) {
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
