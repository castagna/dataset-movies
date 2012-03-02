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

package com.kasabi.data.movies.imdb;

import java.net.URLEncoder;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kasabi.data.movies.BaseLinker;
import com.kasabi.data.movies.Linker;

public class IMDBMovieLinker extends BaseLinker implements Linker {

	private static final Logger log = LoggerFactory.getLogger(IMDBMovieLinker.class) ;
	
	public IMDBMovieLinker(String base) {
		super(base);
	}

	@Override
	public String getURL ( String title_year ) {
		String result = null;
        try {
        	String[] tokens = title_year.split("\t");
        	String title = tokens[0];
        	String year = tokens[1];
        	String url = base + "/find?s=tt&q=" + URLEncoder.encode(title, "UTF-8");
        	String content = retrieve(url);

        	Document document = Jsoup.parse(content);
        	if ( document.title().startsWith("IMDb Title Search") ) {
        		Elements elements = document.select("a:contains(" + title + ")");
        		for (Element element : elements) {
        			String text = element.parent().text();
        			if ( text.contains (year) ) {
	        			String href = element.attr("href");
	        			if ( href.startsWith("/title/") ) {
	        				result = base + href;
	        				break;
	        			}
	        		}
        		}
        	} else {
        		Elements elements = document.select("link[rel=canonical]");
        		if ( !elements.isEmpty() ) {
        			 Element element = elements.first();
        			 result = element.attr("href");
        		}
        	}
        } catch (Exception e) {
			log.error ( e.getMessage(), e );
		}

        log.debug("getURL({}) --> {}", title_year, result);
        return result;
	}

}
