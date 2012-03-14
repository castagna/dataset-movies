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

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.vocabulary.OWL;
import com.kasabi.data.movies.Linker2;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.dbpedia.DBPediaActorLinker2;
import com.kasabi.data.movies.dbpedia.DBPediaDirectorLinker2;
import com.kasabi.data.movies.dbpedia.DBPediaMovieLinker2;
import com.kasabi.data.movies.freebase.FreebaseActorLinker2;
import com.kasabi.data.movies.freebase.FreebaseDirectorLinker2;
import com.kasabi.data.movies.freebase.FreebaseMovieLinker2;

public class RunLinker2 {
	
	private static final Logger log = LoggerFactory.getLogger(RunLinker2.class) ;
	
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
		Location location = new Location ("/opt/datasets/tdb/movies");
		Dataset dataset = TDBFactory.createDataset(location);
		Model model = dataset.getDefaultModel();

		link_freebase(model);
		link_dbpedia(model);
	}

	private static void link_dbpedia(Model model) throws IOException {
		Model result = ModelFactory.createDefaultModel(); 
		link ( result, model, new DBPediaActorLinker2(MoviesCommon.DBPEDIA_NS), actors );
		link ( result, model, new DBPediaDirectorLinker2(MoviesCommon.DBPEDIA_NS), directors );
		link ( result, model, new DBPediaMovieLinker2(MoviesCommon.DBPEDIA_NS), films );
		
		OutputStream out = new BufferedOutputStream( new GZIPOutputStream( new FileOutputStream ("dbpedia.nt.gz") ) );
		result.write(out, "N-TRIPLES");
		out.close();

		result = null;
	}
	
	private static void link_freebase(Model model) throws IOException {
		Model result = ModelFactory.createDefaultModel(); 
		link ( result, model, new FreebaseActorLinker2(MoviesCommon.RDF_FREEBASE_NS), actors );
		link ( result, model, new FreebaseDirectorLinker2(MoviesCommon.RDF_FREEBASE_NS), directors );
		link ( result, model, new FreebaseMovieLinker2(MoviesCommon.RDF_FREEBASE_NS), films ); // we can use the year to disambiguate remakes
		
		OutputStream out = new BufferedOutputStream( new GZIPOutputStream( new FileOutputStream ("freebase.nt.gz") ) );
		result.write(out, "N-TRIPLES");
		out.close();

		result = null;	
	}
	
	private static void link ( Model result, Model model, Linker2 linker, String query ) {
		ResultSet results = select ( model, query );
		while ( results.hasNext() ) {
			QuerySolution solution = results.nextSolution();
			String str = solution.getLiteral("str").getLexicalForm().trim();
			Resource subject = solution.getResource("uri");
			try {
				log.debug("Linking \"{}\" <{}> ...", str, subject.getURI());
				Pair<Resource, Model> pair = linker.link(str, subject, OWL.sameAs);
				if ( pair != null ) {
					result.add(pair.getRight());
				}
			} catch ( Exception e ) {
				log.error(e.getMessage(), e);
			}
		}
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
