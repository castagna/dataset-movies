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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Listing {

	private static final Logger log = LoggerFactory.getLogger(Listing.class);

	protected String filename;
	protected String charset;
	protected String splitPattern;
	protected StringNormalizer normalizer;
	protected Set<String> listing = null;
	
	public Listing(String filename, String charset, String splitPattern, StringNormalizer normalizer ) {
		this.filename = filename;
		this.charset = charset;
		this.splitPattern = splitPattern;
		this.normalizer = normalizer;
	}
	
	public Set<String> getListing() {
		if ( listing == null ) {
			listing = load();
		}
		return listing;
	}

	protected Set<String>load() {
		Set<String> names = new TreeSet<String>();
		BufferedReader in = null;
		try {
			InputStream is = new FileInputStream (filename);
			Reader reader = charset != null ? new InputStreamReader ( is, charset ) : new InputStreamReader ( is );
			in = new BufferedReader ( reader );
			String line;
			while ( (line = in.readLine()) != null ) {
				String[] tokens = line.split(splitPattern);
				for (String token : tokens) {
					String name = token.trim();
					name = normalizer.normalize(name);
					if ( name != null ) {
						names.add(name);
					}
				}
			}
		} catch ( Exception e ) {
			log.error (e.getMessage(), e);
		} finally {
			if ( in != null ) {
				try { in.close(); } catch (IOException e) {}
			}
		}

		return names;
	}
	
}
