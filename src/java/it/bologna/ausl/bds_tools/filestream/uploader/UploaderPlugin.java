package it.bologna.ausl.bds_tools.filestream.uploader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import java.io.InputStream;
import org.json.simple.JSONObject;

public interface UploaderPlugin {

    public String putFile(InputStream file, String path, String fileName) throws FileStreamException;

}
