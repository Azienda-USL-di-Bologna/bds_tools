package it.bologna.ausl.bds_tools.filestream.uploader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.io.InputStream;
import org.json.simple.JSONObject;

/**
 *
 * @author gdm
 */
public class MongoUploader implements UploaderPlugin {

    private final MongoWrapper m;

    public MongoUploader(String mongoUri) throws FileStreamException {

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
        } catch (Exception ex) {
            throw new FileStreamException(ex);
        }
    }

    @Override
    public String putFile(InputStream file, String path, String fileName) throws FileStreamException {
        try {
            return m.put(file, fileName, path, false);
        } catch (Exception ex) {
            throw new FileStreamException(ex);
        }
    }

}
