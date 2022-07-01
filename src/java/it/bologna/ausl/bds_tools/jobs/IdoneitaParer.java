package it.bologna.ausl.bds_tools.jobs;

import it.bologna.ausl.bds_tools.utils.HttpCallResponse;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author Top
 */
@DisallowConcurrentExecution
public class IdoneitaParer implements Job {

    private static final Logger log = LogManager.getLogger(IdoneitaParer.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Dentro Idoneita Parer");
        try (Connection dbConn = UtilityFunctions.getDBConnection();) {
            ServiziAttiviPicoDeteDeli serviziAttivi = getServiziAttivi(dbConn);
            if (!serviziAttivi.areAllDisabled()) {

                String address;
                // todo: controlla orario
                if (serviziAttivi.getPicoStatus()) {
                    address = getAddress(dbConn, "procton");
                    ArrayList<String> docs = getDocs(dbConn, "procton", serviziAttivi);
                    for (String doc : docs) {
                        httpCall(address, doc, "procton", "procton");
                    }
                }
                if (serviziAttivi.getDeteStatus()) {
                    address = getAddress(dbConn, "dete");
                    ArrayList<String> docs = getDocs(dbConn, "dete", serviziAttivi);
                    for (String doc : docs) {
                        httpCall(address, doc, "dete", "dete");
                    }
                }
                if (serviziAttivi.getDeliStatus()) {
                    address = getAddress(dbConn, "deli");
                    ArrayList<String> docs = getDocs(dbConn, "deli", serviziAttivi);
                    for (String doc : docs) {
                        httpCall(address, doc, "deli", "deli");
                    }
                }
            } else {
                log.info("Tutti i servizi sono disattivi");
            }
        } catch (Exception ex) {
            log.error("errore in idoneita Parer", ex);
        }
        log.info("Fine method Idoneita Parer");
    }

    private void httpCall(String address, String guidDoc, String idApp, String idToken) throws IOException {
        HashMap headers = new HashMap();
        headers.put("Content-Type", "application/json");
        headers.put("X-HTTP-Method-Override", "Calcolaidoneita");

        JSONObject body = new JSONObject();
        body.put("idApplicazione", idApp);
        body.put("tokenApplicazione", idToken);
        body.put("guiddoc", guidDoc);

        HttpCallResponse res = UtilityFunctions.httpCallWithHeaders(address,
                null, headers,
                "application/json",
                body.toJSONString().getBytes("UTF-8"),
                600,
                "POST");
        log.info("Ambiente: " + address);
        if (res.isSuccessful()) {
            log.info("lancio idoneita Parer OK");
        } else {
            log.error("documento con guid: "+guidDoc+" non idoneo");
        }
    }

    private ArrayList<String> getDocs(Connection dbConn, String applicazione, ServiziAttiviPicoDeteDeli serviziAttivi) throws SQLException {
        ArrayList<String> res = new ArrayList<>();

        switch (applicazione) {
                case "procton":
                    try (PreparedStatement ps = dbConn.prepareStatement(serviziAttivi.getQueryPico())) {
                        ResultSet r = ps.executeQuery();
                        while (r.next()) {
                            res.add(r.getString("guid_doc"));
                        }
                    }
                    break;
                case "dete":
                    try (PreparedStatement ps = dbConn.prepareStatement(serviziAttivi.getQueryDete())) {
                        ResultSet r = ps.executeQuery();
                        while (r.next()) {
                            res.add(r.getString("guid_doc"));
                        }
                    }
                    break;
                case "deli":
                    try (PreparedStatement ps = dbConn.prepareStatement(serviziAttivi.getQueryDeli())) {
                        ResultSet r = ps.executeQuery();
                        while (r.next()) {
                            res.add(r.getString("guid_doc"));
                        }
                    }
                    break;
                default:
                    return null;
            }

        return res;
    }



    private String getAddress(Connection dbConn, String applicazione) throws SQLException {
        String res = null;
        String sqlQuery = "select val_parametro from bds_tools.parametri_pubblici where nome_parametro= ?";
        try (PreparedStatement ps = dbConn.prepareStatement(sqlQuery)) {
            switch (applicazione) {
                case "procton":
                    ps.setString(1, "idoneitaUrlProcton");
                    break;
                case "dete":
                    ps.setString(1, "idoneitaUrlDete");
                    break;
                case "deli":
                    ps.setString(1, "idoneitaUrlDeli");
                    break;
                default:
                    return null;
            }

            ResultSet r = ps.executeQuery();
            if (r.next()) {
                res = r.getString(1);
            }
        }
        return res;
    }

    private ServiziAttiviPicoDeteDeli getServiziAttivi(Connection dbConn) throws SQLException {
        String sqlQuery = "select id_applicazione,attivo from bds_tools.servizi where nome_servizio=?";

        ServiziAttiviPicoDeteDeli result = new ServiziAttiviPicoDeteDeli();

        try (PreparedStatement ps = dbConn.prepareStatement(sqlQuery)) {
            ps.setString(1, "IdoneitaParerService");
            ResultSet r = ps.executeQuery();

            while (r.next()) {
                String applicazione = r.getString("id_applicazione");
                switch (applicazione) {
                    case "deli":
                        result.setDeliStatus(r.getInt("attivo") != 0);
                        break;
                    case "dete":
                        result.setDeteStatus(r.getInt("attivo") != 0);
                        break;
                    case "procton":
                        result.setPicoStatus(r.getInt("attivo") != 0);
                        break;
                }
            }
        }
        return result;
    }

    public class ServiziAttiviPicoDeteDeli {

        private boolean isActivePicoParer;
        private boolean isActiveDeteParer;
        private boolean isActiveDeliParer;

        public ServiziAttiviPicoDeteDeli() {
            this.isActiveDeliParer = false;
            this.isActiveDeteParer = false;
            this.isActivePicoParer = false;
        }

        public ServiziAttiviPicoDeteDeli(boolean isActivePico,
                boolean isActiveDete,
                boolean isActiveDeli) {
            this.isActiveDeliParer = isActivePico;
            this.isActiveDeliParer = isActiveDeli;
            this.isActiveDeteParer = isActiveDete;
        }

        public boolean getPicoStatus() {
            return this.isActivePicoParer;
        }

        public boolean getDeteStatus() {
            return this.isActiveDeteParer;
        }

        public boolean getDeliStatus() {
            return this.isActiveDeliParer;
        }

        public void setPicoStatus(boolean status) {
            this.isActivePicoParer = status;
        }

        public void setDeteStatus(boolean status) {
            this.isActiveDeteParer = status;
        }

        public void setDeliStatus(boolean status) {
            this.isActiveDeliParer = status;
        }

        public boolean areAllDisabled() {
            return !this.isActiveDeliParer && !this.isActiveDeteParer && !this.isActivePicoParer;
        }

        public String getQueryPico() {
            return  "SELECT REPLACE (g.id_oggetto_origine, 'babel_suite_', '') as guid_doc "
                    + "FROM gd.dati_parer_gddoc dpg, gd.gddocs g "
                    + "WHERE g.id_gddoc = dpg.id_gddoc "
                    + "AND dpg.idoneo_versamento = 0 "
                    + "AND dpg.ritenta_calcolo_idoneita !=0 "
                    + "AND g.codice_registro = 'PG' limit 1 ";
        }

        public String getQueryDete() {
            return  "SELECT REPLACE (g.id_oggetto_origine, 'babel_suite_', '') as guid_doc "
                    + "FROM gd.dati_parer_gddoc dpg, gd.gddocs g "
                    + "WHERE g.id_gddoc = dpg.id_gddoc "
                    + "AND dpg.idoneo_versamento = 0 "
                    + "AND dpg.ritenta_calcolo_idoneita !=0 "
                    + "AND g.codice_registro = 'DETE' ";
        }

        public String getQueryDeli() {
            return  "SELECT REPLACE (g.id_oggetto_origine, 'babel_suite_', '') as guid_doc "
                    + "FROM gd.dati_parer_gddoc dpg, gd.gddocs g "
                    + "WHERE g.id_gddoc = dpg.id_gddoc "
                    + "AND dpg.idoneo_versamento = 0 "
                    + "AND dpg.ritenta_calcolo_idoneita !=0 "
                    + "AND g.codice_registro = 'DELI' ";
        }
    }
}
