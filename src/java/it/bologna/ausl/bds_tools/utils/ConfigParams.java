package it.bologna.ausl.bds_tools.utils;

import it.bologna.ausl.parameters_client.ParametersClient;
import it.bologna.ausl.parameters_client.ParametersDbClient;
import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author gdm
 */
public class ConfigParams {

    private static final Logger log = LogManager.getLogger(ConfigParams.class);

    // è volatile per fare in modo che nessuno possa cacheare l'indirizzo e quindi in fare di ricaricamento di parametri leggere la vecchia mappa

    //-------------------------------------------------------------------------------------------------------//
    // The volatile modifier tells the JVM that writes to the field should always be synchronously flushed to memory, 
    // and that reads of the field should always read from memory. This means that fields marked as volatile can be safely accessed and updated 
    // in a multi-thread application without using native or standard library-based synchronization. 
    // Similarly, reads and writes to volatile fields are atomic. 
    // (This does not apply to >>non-volatile<< long or double fields, which may be subject to "word tearing" on some JVMs.)
    private volatile static Map<String, String> params;
    private static String azienda;
    private static String ambiente;

    public static void initConfigParams() throws IOException {
        ParametersClient client = new ParametersDbClient();

        // passo dallo swap per far si che in fase di ricaricamento non ci siano letture ibride:
        // prima della fine del ricaricamento leggerò sempre i vecchi dati, dopo la fine, sempre i nuovi
        Map<String, String> swap = client.getParameters();
        params = swap;
        azienda = client.getAzienda();
        ambiente = client.getAmbiente();
    }

    public static Map<String, String> getParams() {
        return params;
    }

    public static String getParam(String paramName) {
        return params.get(paramName);
    }

    public static String getAzienda() {
        return azienda;
    }

    public static String getAmbiente() {
        return ambiente;
    }
}