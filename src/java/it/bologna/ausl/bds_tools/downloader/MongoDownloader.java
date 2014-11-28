package it.bologna.ausl.bds_tools.downloader;

import com.mongodb.MongoException;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.MongoWrapperException;
import java.io.InputStream;
import java.net.UnknownHostException;
import org.json.simple.JSONObject;

public class MongoDownloader implements DownloaderPlugin {

    private MongoWrapper m;

    public MongoDownloader(String mongoUri) throws UnknownHostException, MongoException, MongoWrapperException {
        m = new MongoWrapper(mongoUri);
    }

    public InputStream getFile(JSONObject parameters) {
        return m.get((String) parameters.get("file_id"));
    }

    public String getFileName(JSONObject parameters) {
        return m.getFileName((String) parameters.get("file_id"));
    }
}
