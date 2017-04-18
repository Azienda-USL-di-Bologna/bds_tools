package it.bologna.ausl.bds_tools.filestream.downloader;

import it.bologna.ausl.bds_tools.exceptions.FileStreamException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

/**
 *
 * @author gdm
 */
public class RedisDownloader implements DownloaderPlugin {

    private final Jedis redis;

    public RedisDownloader(String redisUri) throws FileStreamException {
        try {
            redis = new Jedis(redisUri);
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }

    @Override
    public InputStream getFile(JSONObject parameters) throws FileStreamException {
        try {
            String key = (String) parameters.get("file_id");
            ByteArrayInputStream bis = new ByteArrayInputStream(redis.get(key.getBytes()));
            return bis;
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }

    @Override
    public String getFileName(JSONObject parameters) throws FileStreamException {
        try {
            return (String) parameters.get("file_name");
        } catch (Exception e) {
            throw new FileStreamException(e);
        }
    }

}
