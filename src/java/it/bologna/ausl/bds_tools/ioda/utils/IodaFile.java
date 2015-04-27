package it.bologna.ausl.bds_tools.ioda.utils;

/**
 *
 * @author gdm
 */
public class IodaFile {
    private String uuid; 
    private String fileName;
    private String mimeType;

    public IodaFile(String uuid, String fileName, String mimeType) {
        this.uuid = uuid;
        this.fileName = fileName;
        this.mimeType = mimeType;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }
}
