package it.bologna.ausl.bds_tools;

import java.io.InputStream;
import java.net.UnknownHostException;

import com.mongodb.MongoException;

import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.MongoWrapperException;


public class MongoDownloader implements DownloaderPlugin {
	private MongoWrapper m;
	
	public MongoDownloader(String mongoUri) throws UnknownHostException, MongoException, MongoWrapperException{
		m=new MongoWrapper(mongoUri);
	}

	public InputStream getFile(String parameters){
		
		return m.get(parameters);
	}
	
	
	

}
