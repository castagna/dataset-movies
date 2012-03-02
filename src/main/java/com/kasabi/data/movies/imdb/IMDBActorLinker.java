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

public class IMDBActorLinker extends BaseLinker implements Linker {

	private static final Logger log = LoggerFactory.getLogger(IMDBActorLinker.class) ;
	
	public IMDBActorLinker(String base) {
		super(base);
	}

	@Override
	public String getURL ( String name ) {
		String result = null;
        try {
        	String url = base + "/find?s=nm&q=" + URLEncoder.encode(name, "UTF-8");
        	String content = retrieve(url);
        	Document document = Jsoup.parse(content);
        	if ( document.title().startsWith("IMDb Name Search") ) {
	        	Elements elements = document.select("b:matches(Exact Matches)");
	        	if ( elements.size() == 1 ) {
	        		Element parent = elements.first().parent();
	        		Elements elements2 = parent.nextElementSibling().select("small:matches(\\(Actor,)");
	        		if ( elements2.size() == 1) {
	        			Element element = elements2.first().parent();
	        			Elements elements3 = element.select("a[href^=/name/]");
	        			if ( !elements3.isEmpty() ) {
	        				result = base + elements3.first().attr("href");
	        			}   			
	        		} 
	        		if ( elements2.size() == 0 ) {
	        			elements2 = parent.nextElementSibling().select("small:matches(\\(Actress,)");
		        		if ( elements2.size() == 1) {
		        			Element element = elements2.first().parent();
		        			Elements elements3 = element.select("a[href^=/name/]");
		        			if ( !elements3.isEmpty() ) {
		        				result = base + elements3.first().attr("href");
		        			} 
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

        log.debug("getURL({}) --> {}", name, result);
        return result;
	}

}
