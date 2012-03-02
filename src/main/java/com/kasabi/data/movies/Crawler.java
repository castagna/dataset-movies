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
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.TreeSet;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;


public class Crawler {

	protected static final Logger log = LoggerFactory.getLogger(Crawler.class) ;
	
	protected String base;
	protected String startingPage;
	protected String selectItem;
	protected String selectItemURI;
	protected String selectNextPage;
	private int maxNumRetries = 5;
	protected Cache cache = null;
	protected boolean retrieveAll = true;
	private String charset = null;
	private Scraper scraper = null;
	
	public Crawler (String base, String startingPage, String selectItem, String selectItemURI, String selectNextPage, Scraper scraper) {
		log.debug("Crawler({},{},{},{})", new Object[]{base, startingPage, selectItemURI, selectNextPage});
		this.base = base;
		this.startingPage = startingPage;
		this.selectItem = selectItem;
		this.selectItemURI = selectItemURI;
		this.selectNextPage = selectNextPage;
		this.cache = new Cache(base, new File("data/cache"));
		this.scraper = scraper;
	}
	
	public void setRetrieveAll ( boolean retrieveAll ) {
		log.debug("setRetrieveAll({})", retrieveAll);
		this.retrieveAll = retrieveAll ;
	}

	public void setCharset(String charset) {
		log.debug("setCharset({})", charset);
		this.charset = charset ;
	}
	
	public Set<String> crawl()  {
		log.debug("crawl()");
		Set<String> pages = new TreeSet<String>();
		String page = startingPage;
		String previousPage = null;
		while ( ( page != null ) && ( !page.equals(previousPage) ) ) {
			String url = base + relativize(base, page);
			log.info("crawl() --> retrieving {} ...", url); 
			Document doc = retrieve (url);
			if ( doc != null ) {
				Elements elements = doc.select(selectItem);
				for (Element element : elements) {
					String href = relativize(base, element.select(selectItemURI).first().attr("href"));
					log.debug("crawl() --> found {}", href); 
					pages.add(href);
					
					@SuppressWarnings("unused")
					Model model = scraper.scrape(element);

					// if ( log.isDebugEnabled() ) model.write(System.out, "TURTLE");

					if ( retrieveAll ) retrieve ( base + href );
				}
				log.debug("craw() --> found {} pages so far...", pages.size());
				previousPage = page;
				page = getNextPage (doc);					
			}
		}
		log.debug("crawl() --> found {} pages.", pages.size());
		return pages;
	}
	
	protected String relativize ( String base, String href ) {
		if ( href.startsWith(base) ) {
			return href.substring(base.length());
		} else {
			return href;
		}
	}
	
	protected Document retrieve ( String url ) {
		log.debug("retrieve({})", url);
		return retrieve ( cache, url, maxNumRetries, charset );
	}
	
	public static Document retrieve ( Cache cache, String url ) {
		log.debug("retrieve({},{})", cache, url);
		return retrieve ( cache, url, 5, null );
	}
	
	public static Document retrieve ( Cache cache, String url, String charset ) {
		log.debug("retrieve({},{},{})", new Object[]{cache, url, charset});
		return retrieve ( cache, url, 5, charset );
	}
	
	public static Document retrieve ( Cache cache, String url, int maxNumRetries, String charset ) {
		log.debug("retrieve({},{},{})", new Object[]{cache, url, String.valueOf(maxNumRetries)});
		Document document = null;
		
		if ( cache.has(url) ) {
			log.debug("retrieve(...,{},...) --> content is in the cache", url);
			String content = cache.get(url);
			document = Jsoup.parse(content);
		} else {
			log.info("retrieve(...,{},...) --> retrieving ...", url);
			int retry = 0;
			while ( document == null && retry<maxNumRetries ) {
	            retry++;
    			delay (500 * retry);
    			try {
    				if ( charset == null ) {
    	                document = Jsoup.connect(url).userAgent("Mozilla").timeout(10*1000).get();    					
    				} else {
    					log.debug("retrieve(...,{},...) --> retrieving using charset {} ...", url, charset);
    					document = Jsoup.parse(new URL(url).openStream(), charset, url);
    				}
	            } catch (IOException e) {
	            	log.warn("retrieve(...,{},...) --> {}. Retrying, attempt {} ...", new Object[]{url, e.getMessage(), retry});
	            }
			}
			
			if ( document != null ) {
				cache.put( url, document.toString());
			} else {
				log.error("retrieve(...,{},...) --> Unable to retrieve url, tried {} times.", url, retry);
			}
		}

		return document;
	}
	
	public static void delay ( int milliseconds ) {
		log.debug("delay({})", milliseconds);
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
			log.error("delay({}): {}", milliseconds, e.getMessage());
		}
	}
	
	protected String getNextPage(Document doc) {
		Elements elements = doc.select(selectNextPage);
		String href = null;
		if ( elements.size() > 0 ) {
			href = elements.get(0).attributes().get("href").replaceAll(" ", "+");
		}
		log.debug("getNextPage({}) --> {}", doc.baseUri(), href);	
		return href;
	}
}
