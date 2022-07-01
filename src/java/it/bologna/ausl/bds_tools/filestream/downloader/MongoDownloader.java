package it.bologna.ausl.bds_tools.filestream.downloader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.io.InputStream;
import org.json.simple.JSONObject;

public class MongoDownloader implements DownloaderPlugin {

    private final MongoWrapper m;

    public MongoDownloader(String mongoUri) throws FileStreamException {
        try {
            boolean useMinIO = ApplicationParams.getUseMinIO();
            JSONObject minIOConfig = ApplicationParams.getMinIOConfig();
            String codiceAzienda = ApplicationParams.getCodiceAzienda();
            Integer maxPoolSize = Integer.parseInt(minIOConfig.get("maxPoolSize").toString());
            m = MongoWrapper.getWrapper(
                    useMinIO,
                    mongoUri,
                    minIOConfig.get("DBDriver").toString(),
                    minIOConfig.get("DBUrl").toString(),
                    minIOConfig.get("DBUsername").toString(),
                    minIOConfig.get("DBPassword").toString(),
                    codiceAzienda,
                    maxPoolSize,
                    null);
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }

    @Override
    public InputStream getFile(JSONObject parameters) throws FileStreamException {
        try {
            return m.get((String) parameters.get("file_id"));
        } catch (Exception e) {
            throw new FileStreamException("errore nella getFile", e);
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
            throw new FileStreamException("errore nella getFileName", e);
        }
    }
}
