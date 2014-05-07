package it.bologna.ausl.bds_tools.downloader;

import com.mongodb.MongoException;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.MongoWrapperException;
import java.io.InputStream;
import java.net.UnknownHostException;


public class MongoDownloader implements DownloaderPlugin {
private MongoWrapper m;
	
    public MongoDownloader(String mongoUri) throws UnknownHostException, MongoException, MongoWrapperException {
        m=new MongoWrapper(mongoUri);
    }

    public InputStream getFile(String parameters) {
        return m.get(parameters);
    }

    public String getFileName(String parameters) {
        return m.getFileName(parameters);
    }
}
