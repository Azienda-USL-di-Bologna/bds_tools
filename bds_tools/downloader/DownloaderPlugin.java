package it.bologna.ausl.bds_tools.downloader;

import java.io.InputStream;

public interface DownloaderPlugin {

    public InputStream getFile(String parameters);
    public String getFileName(String parameters);

}
