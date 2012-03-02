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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFErrorHandler;
import com.hp.hpl.jena.shared.JenaException;
import com.kasabi.data.movies.MoviesCommon;

public class FreebaseMoviesCrawlerProcessor {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		File path = new File ( "data/cache/rdf.freebase.com/rdf/" );
		BufferedWriter out = new BufferedWriter( new OutputStreamWriter ( new GZIPOutputStream ( new FileOutputStream ("/tmp/freebase.nt.gz") ) ) );
		int count = 0;
		for ( final File file : path.listFiles() ) {
			if ( file.isFile() ) {
				Model model = MoviesCommon.createModel();
				model.getReader().setErrorHandler(new RDFErrorHandler() {
					@Override
					public void warning(Exception e) {
						System.err.println("Warning: " + file.getName() + " " + e.getMessage());
					}			
					@Override
					public void fatalError(Exception e) {
						System.err.println("Fatal: " + file.getName() + " " + e.getMessage());
					}
					@Override
					public void error(Exception e) {
						System.err.println("Error: " + file.getName() + " " + e.getMessage());
					}
				});
				model.getWriter().setErrorHandler(new RDFErrorHandler() {
					@Override
					public void warning(Exception e) {
						System.err.println("Warning: " + file.getName() + " " + e.getMessage());
					}			
					@Override
					public void fatalError(Exception e) {
						System.err.println("Fatal: " + file.getName() + " " + e.getMessage());
					}
					@Override
					public void error(Exception e) {
						System.err.println("Error: " + file.getName() + " " + e.getMessage());
					}
				});
				InputStream in = null;
				try {
					in = new FileInputStream ( file );
					model.read(new FileInputStream(file), null);
					model.write(out, "N-TRIPLES");
				} catch (JenaException e) {					
					System.err.println("Error: " + file.getName() + " " + e.getMessage());
					file.delete();
				} catch (FileNotFoundException e) {
					System.err.println("Error: " + file.getName() + " " + e.getMessage());
					file.delete();
				} finally {
					if ( in != null ) try { in.close(); } catch (IOException e) {}
				}
				count++;
				if ( count % 100 == 0 ) System.out.print(".");
				if ( count % 1000 == 0 ) System.out.println();
			}
		}
		out.flush();
		out.close();
	}

}
