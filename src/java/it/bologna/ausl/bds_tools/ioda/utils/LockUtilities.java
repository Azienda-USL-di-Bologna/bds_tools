package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.masterchefclient.exceptions.RedisClientException;
import it.bologna.ausl.redis.RedisClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
public class LockUtilities {
    private static final Logger log = LogManager.getLogger(LockUtilities.class);
    
    private final String LOCK_KEY_TEMPLATE = "lock_[tipo_oggetto]_[id_oggetto]_[ambito]_[server]";
    private final String DEFAULT_LOCK_VALUE_TEMPLATE = "[tipo_oggetto]_[id_sessione]";
    private final String AMBITO_GD_DOC = "ambito_gddoc";
    
    private final String key;
    private final String value;
    private final RedisClient redisClient;
    private final int timeoutSeconds;

    public LockUtilities(String idOggetto, String tipoOggetto, String idSessione, String server, String redisHost, int timeoutSeconds) {
        this.key = buildLockKey(idOggetto, tipoOggetto, server);
        this.value = buildDefaultLockValue(tipoOggetto, idSessione);
        this.timeoutSeconds = timeoutSeconds;
        redisClient = new RedisClient(redisHost, null);
    }
    
    public LockUtilities(String idOggetto, String tipoOggetto, String idSessione, String server, String redisHost) {
        this.key = buildLockKey(idOggetto, tipoOggetto, server);
        this.value = buildDefaultLockValue(tipoOggetto, idSessione);
        this.timeoutSeconds = -1;
        redisClient = new RedisClient(redisHost, null);
    }
    
    private String buildLockKey(String idOggetto, String tipoOggetto, String server) {
        String lockKey = LOCK_KEY_TEMPLATE.replace("[id_oggetto]", idOggetto).
                                           replace("[tipo_oggetto]", tipoOggetto).
                                           replace("[ambito]", AMBITO_GD_DOC).
                                           replace("[server]", server);
        return lockKey;
    }
    
    private String buildDefaultLockValue(String tipoOggetto, String idSessione) {
        String lockValue = DEFAULT_LOCK_VALUE_TEMPLATE.replace("[tipo_oggetto]", tipoOggetto).
                                                       replace("[id_sessione]", idSessione);
        return lockValue;
    }
    
    public boolean getLock() throws Exception {
        boolean locked = false;
        if (redisClient.setnx(key, value) == 1) {
            locked = true;
            if (timeoutSeconds != -1) {
                if (redisClient.expire(key, timeoutSeconds) != 1) {
                    log.error("impossibile settare il timeout. La chiave esiste?");
                    throw new RedisClientException("impossibile settare il timeout. La chiave esiste?");
                }
            }
            else 
                locked = true;
        }
        return locked;
    }

    public boolean isLocked() throws Exception {
        String keyValue = redisClient.getKey(key);
        return keyValue != null && !keyValue.equals("");
    }
    
    public boolean isExpired() throws Exception {
        return redisClient.isExpired(key, value, timeoutSeconds);
    }
    
    public void deleteLock(boolean forceDelete) throws Exception {
        if (forceDelete || !redisClient.isExpired(key, value, timeoutSeconds)) {
            redisClient.del(key);
        }
    }
}
