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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import com.kasabi.data.movies.ActorNameNormalizer;
import com.kasabi.data.movies.DirectorNameNormalizer;
import com.kasabi.data.movies.Linker;
import com.kasabi.data.movies.Listing;
import com.kasabi.data.movies.MovieListing;
import com.kasabi.data.movies.MovieTitleNormalizer;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.dbpedia.DBPediaActorLinker;
import com.kasabi.data.movies.dbpedia.DBPediaDirectorLinker;
import com.kasabi.data.movies.dbpedia.DBPediaMovieLinker;
import com.kasabi.data.movies.freebase.FreebaseActorLinker;
import com.kasabi.data.movies.freebase.FreebaseDirectorLinker;
import com.kasabi.data.movies.freebase.FreebaseMovieLinker;
import com.kasabi.data.movies.imdb.IMDBActorLinker;
import com.kasabi.data.movies.imdb.IMDBDirectorLinker;
import com.kasabi.data.movies.imdb.IMDBMovieLinker;

public class RunLinker {

	public static final String PATH = "/opt/datasets/movies/";
	
	public static void main(String[] args) throws IOException {
		titles();
		directors();
		actors();
	}
	
	private static void directors() throws IOException {
		Listing listing = new Listing(PATH + "directors-original.txt", "ISO-8859-1", ",", new DirectorNameNormalizer());
		Set<String> directors = listing.getListing();
		write ( directors, PATH + "directors.txt" );
		write ( directors, new FreebaseDirectorLinker(MoviesCommon.RDF_FREEBASE_NS), PATH + "directors-links-freebase.tsv");
		write ( directors, new IMDBDirectorLinker(MoviesCommon.IMDB_NS), PATH + "directors-links-imdb.tsv" );
		write ( directors, new DBPediaDirectorLinker(MoviesCommon.DBPEDIA_NS), PATH + "directors-links-dbpedia.tsv");
	}
	
	private static void actors() throws IOException {
		Listing listing = new Listing(PATH + "actors-original.txt", "ISO-8859-1", ",", new ActorNameNormalizer());
		Set<String> actors = listing.getListing();
		write ( actors, PATH + "actors.txt" );
		write ( actors, new FreebaseActorLinker(MoviesCommon.RDF_FREEBASE_NS), PATH + "actors-links-freebase.tsv");
		write ( actors, new IMDBActorLinker(MoviesCommon.IMDB_NS), PATH + "actors-links-imdb.tsv" );
		write ( actors, new DBPediaActorLinker(MoviesCommon.DBPEDIA_NS), PATH + "actors-links-dbpedia.tsv");
	}
	
	private static void titles() throws IOException {
		Listing listing = new MovieListing(PATH + "titles-original.txt", "ISO-8859-1", ",", new MovieTitleNormalizer());
		Set<String> titles = listing.getListing();
		write ( titles, PATH + "titles.txt" );
		write ( titles, new FreebaseMovieLinker(MoviesCommon.RDF_FREEBASE_NS), PATH + "titles-links-freebase.tsv");
		write ( titles, new IMDBMovieLinker(MoviesCommon.IMDB_NS), PATH + "titles-links-imdb.tsv" );
		write ( titles, new DBPediaMovieLinker(MoviesCommon.DBPEDIA_NS), PATH + "titles-links-dbpedia.tsv");
	}
	
	private static void write ( Set<String> names, String filename ) throws FileNotFoundException {
		PrintWriter out = new PrintWriter ( filename );
		for (String name : names) {
			out.println(name);
		}
		out.close();
	}
	
	private static void write ( Set<String> names, Linker linker, String filename ) throws FileNotFoundException {
		PrintWriter out = new PrintWriter ( filename );
		for (String name : names) {
			String url = linker.getURL(name);
			out.print(name + "\t");
			if ( url != null ) {
				out.print(url);
			}
			out.println();
		}
		out.close();
	}

}
