package it.bologna.ausl.bds_tools.downloader;

import com.mongodb.MongoException;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.MongoWrapperException;
import java.io.InputStream;
import java.net.UnknownHostException;
import org.json.simple.JSONObject;

public class MongoDownloader implements DownloaderPlugin {

    private final MongoWrapper m;

    public MongoDownloader(String mongoUri) throws UnknownHostException, MongoException, MongoWrapperException {
        m = new MongoWrapper(mongoUri);
    }

    @Override
    public InputStream getFile(JSONObject parameters) {
        return m.get((String) parameters.get("file_id"));
    }

    @Override
    public String getFileName(JSONObject parameters) {
        String fileName = (String) parameters.get("file_name");
        if (fileName == null || fileName.equals(""))
            fileName = m.getFileName((String) parameters.get("file_id"));
        return fileName;
    }
}
