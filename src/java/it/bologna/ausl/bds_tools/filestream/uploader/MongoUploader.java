package it.bologna.ausl.bds_tools.filestream.uploader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import java.io.InputStream;

/**
 *
 * @author gdm
 */
public class MongoUploader implements UploaderPlugin {

    private final MongoWrapper m;

    public MongoUploader(String mongoUri) throws FileStreamException {

        try {
            m = new MongoWrapper(mongoUri);
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
