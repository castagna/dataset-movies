package com.kasabi.data.movies.imdb;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import com.kasabi.data.movies.Crawler;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.Scraper;

public class IMDBCrawler {

	public static void main(String[] args) throws IOException {
		OutputStream out = new BufferedOutputStream ( new GZIPOutputStream( new FileOutputStream ( "imdb2.nt.gz") ) );
		Scraper scraper = new IMDBMovieScraper(MoviesCommon.IMDB_NS, out);
		Crawler crawler = new Crawler(MoviesCommon.IMDB_NS, "/search/title?countries=gb&sort=moviemeter,asc", "table.results tr[class~=detailed]", "td.title a[href^=/title/]", "a:matches(Next)", scraper);
		crawler.setRetrieveAll(false);
		crawler.crawl();
		crawler = new Crawler(MoviesCommon.IMDB_NS, "/search/title?countries=gb&sort=moviemeter,desc", "table.results tr[class~=detailed]", "td.title a[href^=/title/]", "a:matches(Next)", scraper);
		crawler.setRetrieveAll(false);
		crawler.crawl();
		out.flush();
		out.close();
	}
	
}
