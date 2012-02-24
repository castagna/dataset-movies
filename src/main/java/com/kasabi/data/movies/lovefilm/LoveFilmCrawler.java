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

package com.kasabi.data.movies.lovefilm;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.zip.GZIPOutputStream;


public class LoveFilmCrawler {

	public static String BASE = "http://www.lovefilm.com";
	
	public static void main(String[] args) throws IOException {
		OutputStream out = new BufferedOutputStream ( new GZIPOutputStream( new FileOutputStream ( "lovefilm.nt.gz") ) );
		Crawler crawler = new Crawler(BASE, "/browse/film/p1/?rows=50", "div.fl_detail_info", "h2 > a[href^=http://www.lovefilm.com/film/]", "a:matches(Next)", out);
		crawler.setRetrieveAll(false);
		Set<String> pages = crawler.crawl();
		out.flush();
		out.close();
	}

}
