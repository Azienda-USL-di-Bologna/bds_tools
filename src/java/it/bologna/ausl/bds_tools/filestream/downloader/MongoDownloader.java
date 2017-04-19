package it.bologna.ausl.bds_tools.filestream.downloader;

import com.mongodb.MongoException;
import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.exceptions.MongoWrapperException;
import java.io.InputStream;
import java.net.UnknownHostException;
import org.json.simple.JSONObject;

public class MongoDownloader implements DownloaderPlugin {

    private final MongoWrapper m;

    public MongoDownloader(String mongoUri) throws FileStreamException {
        try {
            m = new MongoWrapper(mongoUri);
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }

    @Override
    public InputStream getFile(JSONObject parameters) throws FileStreamException {
        try {
            return m.get((String) parameters.get("file_id"));
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }

    @Override
    public String getFileName(JSONObject parameters) throws FileStreamException {
        try {
            String fileName = (String) parameters.get("file_name");
            if (fileName == null || fileName.equals("")) {
                fileName = m.getFileName((String) parameters.get("file_id"));
            }
            return fileName;
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }
}
