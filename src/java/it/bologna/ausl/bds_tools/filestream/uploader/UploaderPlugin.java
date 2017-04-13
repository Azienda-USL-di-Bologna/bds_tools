package it.bologna.ausl.bds_tools.filestream.uploader;

import java.io.InputStream;
import org.json.simple.JSONObject;

public interface UploaderPlugin {

    public InputStream putFile(JSONObject metadata, InputStream file);

}
