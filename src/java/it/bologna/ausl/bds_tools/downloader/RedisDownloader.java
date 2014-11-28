/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools.downloader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Administrator
 */
public class RedisDownloader implements DownloaderPlugin {

    private Jedis redis;

    public RedisDownloader(String redisUri) {
        redis = new Jedis(redisUri);
    }

    public InputStream getFile(JSONObject parameters) {
        String key = (String) parameters.get("file_id");
        ByteArrayInputStream bis = new ByteArrayInputStream(redis.get(key.getBytes()));
        return bis;
    }

    public String getFileName(JSONObject parameters) {

        return (String) parameters.get("file_name");
    }

}
