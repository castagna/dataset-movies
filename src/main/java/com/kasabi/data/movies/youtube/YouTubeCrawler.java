package com.kasabi.data.movies.youtube;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import com.kasabi.data.movies.Crawler;
import com.kasabi.data.movies.MoviesCommon;
import com.kasabi.data.movies.Scraper;

public class YouTubeCrawler {

	public static void main(String[] args) throws IOException {
		OutputStream out = new BufferedOutputStream ( new GZIPOutputStream( new FileOutputStream ( "youtube.nt.gz") ) );
		Scraper scraper = new YouTubeVideoScraper(MoviesCommon.YOUTUBE_NS, out);
		Crawler crawler = new Crawler(MoviesCommon.YOUTUBE_NS, "/user/" + MoviesCommon.YOUTUBE_CHANNEL + "/videos?sort=dd&view=0&page=1", "h3.video-title", "a[href^=/watch]", "a:matches(Next)", scraper);
		crawler.setRetrieveAll(false);
		crawler.crawl();
		out.flush();
		out.close();
	}
	
}
