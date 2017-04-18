package it.bologna.ausl.bds_tools.filestream.downloader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import java.io.InputStream;
import org.json.simple.JSONObject;

public interface DownloaderPlugin {

    public InputStream getFile(JSONObject parameters) throws FileStreamException;

    public String getFileName(JSONObject parameters) throws FileStreamException;

}
