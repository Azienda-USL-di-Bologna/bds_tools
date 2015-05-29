package it.bologna.ausl.bds_tools.downloader;

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

    public RedisDownloader(String redisUri) {
        redis = new Jedis(redisUri);
    }

    @Override
    public InputStream getFile(JSONObject parameters) {
        String key = (String) parameters.get("file_id");
        ByteArrayInputStream bis = new ByteArrayInputStream(redis.get(key.getBytes()));
        return bis;
    }

    @Override
    public String getFileName(JSONObject parameters) {
        return (String) parameters.get("file_name");
    }

}
