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

package cmd;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.openjena.atlas.lib.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.vocabulary.OWL;
import com.kasabi.data.movies.Linker2;
import com.kasabi.data.movies.MoviesCommon;

public class RunIMDBSlicer {
	
	private static final Logger log = LoggerFactory.getLogger(RunIMDBSlicer.class) ;
	
	private static String prefixes = 
		"PREFIX movies: <" + MoviesCommon.KASABI_MOVIES_SCHEMA + "> " +
		"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " + 
		"PREFIX bibo: <http://purl.org/ontology/bibo/> " +
		"PREFIX dct: <http://purl.org/dc/terms/> " +
		"PREFIX linkedmdb: <http://data.linkedmdb.org/resource/movie/>";
	private static String directors = prefixes + "SELECT DISTINCT ?str ?uri { ?uri a foaf:Person . ?uri foaf:name ?str . ?film movies:directed ?uri . } ORDER BY ?str ";
	private static String actors = prefixes + "SELECT DISTINCT ?str ?uri { ?uri a foaf:Person . ?uri foaf:name ?str . ?uri movies:featured_in ?film . } ORDER BY ?str ";
	private static String films = prefixes + "SELECT DISTINCT ?str ?uri { ?uri a bibo:Film . ?uri dct:title ?str . } ORDER BY ?str ";

	public static void main(String[] args) throws IOException {
		Model model = TDBFactory.createDataset(new Location ("/opt/datasets/tdb/movies")).getDefaultModel();
		Model imdb = TDBFactory.createDataset(new Location ("/opt/datasets/tdb/imdb")).getDefaultModel();

		Model result = slice_imdb(imdb, model);
		OutputStream out = new BufferedOutputStream( new GZIPOutputStream( new FileOutputStream ("imdb-slice.nt.gz") ) );
		result.write(out, "N-TRIPLES");
		out.close();
	}

	private static Model slice_imdb (Model imdb, Model model) throws IOException {
		Model result = ModelFactory.createDefaultModel();
		String[] queries = { actors, directors, films };
		for (String query : queries) {
			ResultSet results = select ( model, query );
			while ( results.hasNext() ) {
				QuerySolution solution = results.nextSolution();
				String str = solution.getLiteral("str").getLexicalForm().trim();
				Resource subject = solution.getResource("uri");
				result.add(imdb.listStatements(subject, (Property)null, (RDFNode)null));
			}
		}
		return result;
	}

	
	public static ResultSet select( Model model, String query ) {
		QueryExecution qexec = QueryExecutionFactory.create(QueryFactory.create(query), model);
		ResultSetRewindable results = null;
		try {
			results = ResultSetFactory.makeRewindable(qexec.execSelect());
		} finally {
			qexec.close();
		}
		return results;
	}

}
