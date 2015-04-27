package it.bologna.ausl.bds_tools.utils;

import java.util.List;

/**
 *
 * @author gdm
 */
public class SupportedFile {
    private String mimeType;
    private String extension;
    private boolean pdfConvertible;

    public SupportedFile(String mimeType, String extension, boolean pdfConvertible) {
        this.mimeType = mimeType;
        this.extension = extension;
        this.pdfConvertible = pdfConvertible;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public boolean isPdfConvertible() {
        return pdfConvertible;
    }

    public void setPdfConvertible(boolean pdfConvertible) {
        this.pdfConvertible = pdfConvertible;
    }
    
    public static boolean isSupported(List<SupportedFile> supportedFiles,String mimeType) {
        return getSupportedFile(supportedFiles, mimeType) != null;
    }
    
    public static SupportedFile getSupportedFile(List<SupportedFile> supportedFiles, String mimeType) {
        SupportedFile res = null;
        for (SupportedFile f: supportedFiles) {
            if (f.mimeType.equalsIgnoreCase(mimeType))
                res = f;
        }
        return res;
    }
}
