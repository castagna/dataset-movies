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

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileManager;
import com.kasabi.data.movies.BaseLinker2;
import com.kasabi.data.movies.Linker2;
import com.kasabi.data.movies.MoviesCommon;

public abstract class DBPediaBaseLinker2 extends BaseLinker2 implements Linker2 {

	private final FileManager fm;
	protected final HttpClient httpclient;
	
	public DBPediaBaseLinker2(String base) {
		super(base);
		this.fm = MoviesCommon.getFileManager(base); 
		this.httpclient = new DefaultHttpClient();
	}

	protected Pair<String, Model> get(HttpClient httpclient, String type, String string) {
		Model model = MoviesCommon.createModel();
		String uri = getURI(httpclient, type, string);
		if ( uri != null ) {
			InputStream in = fm.openNoMap(uri);
			model.read(in, base);			
		}
		return new Pair<String, Model>(uri, model);
	}

	protected String getURI(HttpClient httpclient, String type, String string) {
		String uri = null;
        try {
        	String queryClass = type != null ? "&QueryClass=" + URLEncoder.encode(type, "UTF-8") : "";
        	String queryString = "?QueryString=" + URLEncoder.encode(string, "UTF-8");
            HttpGet httpget = new HttpGet("http://lookup.dbpedia.org/api/search.asmx/KeywordSearch" + queryString + queryClass);
            ResponseHandler<String> responseHandler = new BasicResponseHandler();
            String responseBody = httpclient.execute(httpget, responseHandler);
            Document document = Jsoup.parse(responseBody);
            Elements elements = document.select("result > uri");
            if ( !elements.isEmpty() ) {
            	uri = elements.first().text();
            }
        } catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return uri;
	}

	
}
