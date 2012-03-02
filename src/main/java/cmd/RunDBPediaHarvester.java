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

import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.dbpedia.DBPediaRDFHarvester;
import com.kasabi.data.movies.dbpedia.DBPediaRDFMovieHarvester;

public class RunDBPediaHarvester {

	public static void main(String[] args) throws IOException {
		OutputStream out = new BufferedOutputStream ( new GZIPOutputStream( new FileOutputStream ( "dbpedia.nt.gz") ) );
		new DBPediaRDFMovieHarvester(MoviesCommon.DBPEDIA_NS, RunLinker.PATH + "titles-links-dbpedia.tsv", null, "/film", ResourceFactory.createResource(MoviesCommon.LINKEDMDB_NS + "Film"), out).harvest();
		new DBPediaRDFHarvester(MoviesCommon.DBPEDIA_NS, RunLinker.PATH + "directors-links-dbpedia.tsv", null, "/director", ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + "Director"), out).harvest();
		new DBPediaRDFHarvester(MoviesCommon.DBPEDIA_NS, RunLinker.PATH + "actors-links-dbpedia.tsv", null, "/actor", ResourceFactory.createResource(MoviesCommon.KASABI_ACTORS_NS + "Actor"), out).harvest();
		out.close();
	}

}
