package it.bologna.ausl.bds_tools.utils;

import java.io.IOException;
import java.io.InputStream;
import okhttp3.Response;

/**
 *
 * @author Spritz
 */
public class HttpCallResponse {

    private okhttp3.Response response;

    public HttpCallResponse(Response response) {
        this.response = response;
    }

    public String getString() throws IOException {
        return response.body().string();
    }

    public InputStream getByteStream() {
        return response.body().byteStream();
    }

    public byte[] getBytes() throws IOException {
        return response.body().bytes();
    }

    public int getStatus() {
        return response.code();
    }

    public boolean isSuccessful() {
        return response.isSuccessful();
    }

    public String getHeader(String headerName) {
        return response.header(headerName);
    }
}
