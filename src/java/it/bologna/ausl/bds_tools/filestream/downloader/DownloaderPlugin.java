package it.bologna.ausl.bds_tools.filestream.downloader;

import java.io.InputStream;
import org.json.simple.JSONObject;

public interface DownloaderPlugin {

    public InputStream getFile(JSONObject parameters);

    public String getFileName(JSONObject parameters);

}
