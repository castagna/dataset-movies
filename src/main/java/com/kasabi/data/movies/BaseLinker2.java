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

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.openjena.atlas.io.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseLinker2 implements Linker2 {
	
	private static final Logger log = LoggerFactory.getLogger(BaseLinker2.class) ;
	protected final String base;
	private final Cache cache;
	private final HttpClient httpclient;
	
	public BaseLinker2 ( String base ) {
		this.base = base;
		this.cache = new Cache(base, new File("data/cache"));
		this.httpclient = new DefaultHttpClient();
	}

	public String retrieve ( String url ) {
    	String content = null;
    	try {
        	if ( cache.has(url) ) {
                content = cache.get(url);
        	} else {
                HttpGet httpget = new HttpGet(url);
                HttpResponse response = httpclient.execute(httpget);
                content = IO.readWholeFileAsUTF8(response.getEntity().getContent());
                cache.put(url, content);
        	}    		
    	} catch ( Exception e ) {
    		log.error(e.getMessage(), e);
    	}
    	return content;
	}
	
}
