package it.bologna.ausl.bds_tools.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.bologna.ausl.bds_tools.exceptions.ConvertPdfExeption;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.exceptions.VersatoreParerException;
import it.bologna.ausl.bds_tools.ioda.utils.IodaDocumentUtilities;
import it.bologna.ausl.bds_tools.ioda.utils.IodaFascicolazioniUtilities;
import it.bologna.ausl.bds_tools.jobs.utils.ServiceRequestInformation;
import it.bologna.ausl.bds_tools.jobs.utils.VersatoreParerUtils;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.BagProfiloArchivistico;
import it.bologna.ausl.ioda.iodaobjectlibrary.DatiParerGdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDocSessioneVersamento;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.masterchefclient.SendToParerParams;
import it.bologna.ausl.masterchefclient.SendToParerResult;
import it.bologna.ausl.masterchefclient.UpdateBabelParams;
import it.bologna.ausl.masterchefclient.WorkerData;
import it.bologna.ausl.masterchefclient.WorkerResponse;
import it.bologna.ausl.masterchefclient.WorkerResult;
import it.bologna.ausl.masterchefclient.errors.ErrorDetails;
import it.bologna.ausl.masterchefclient.errors.SendToParerErrorDetails;
import it.bologna.ausl.masterchefclient.exceptions.MasterChefClientException;
import it.bologna.ausl.redis.RedisClient;
import it.bologna.ausl.riversamento.builder.IdentityFile;
import it.bologna.ausl.riversamento.builder.InfoDocumento;
import it.bologna.ausl.riversamento.builder.ProfiloArchivistico;
import it.bologna.ausl.riversamento.builder.UnitaDocumentariaBuilder;
import it.bologna.ausl.riversamento.builder.oggetti.DatiSpecifici;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import javax.naming.NamingException;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author andrea
 */
@DisallowConcurrentExecution
public class VersatoreParer implements Job {

    private static final Logger log = LogManager.getLogger(VersatoreParer.class);

    private static final String GENERATE_INDE_NUMBER_PARAM_NAME = "generateidnumber";
    private static final String INDE_DOCUMENT_ID_PARAM_NAME = "document_id";
    public static final String INDE_DOCUMENT_GUID_PARAM_NAME = "document_guid";

    public static final String DATE_PATTERN_STANDARD = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String DATE_PATTERN_FOR_USER = "dd-MM-yyyy HH:mm:ss";

    private boolean canSendPicoUscita, canSendPicoEntrata, canSendDete, canSendDeli;
    private boolean canSendRegistroGiornaliero, canSendRegistroAnnuale;
    private boolean canSendRgPico, canSendRgDete, canSendRgDeli;
    private int limit;

    private String versione;
    private String ambiente, ente, strutturaVersante, userID;
    private String tipoComponenteDefault, codifica;
    private String dateFrom, dateTo;
    private String role;
    private boolean useFakeId;

    private String idApplicazione, tokenApplicazione;
    private String prefix;

    private final String ID_APPLICAZIONE = "gedi";
    private final String SERVIZIO_IDONEITA = "IdoneitaParerService";
    private final String NOME_SERVIZIO = "VersatoreParer";

    private final String SCHEDULATO = "SCHEDULATO";
    private final String AMMINISTRATIVO = "AMMINISTRATIVO";
    private final String CRUSCOTTO_PARER = "CRUSCOTTO_PARER";

    /**
     * Questo ?? il metodo di partenza che determina se il servizio deve girare
     * perch?? chiamato dallo schedulatore o perch?? ?? stata fatta una richiesta
     * "a volere". Dopo i controlli ci accesso il metodo doJob(...) si
     * incaricher?? di gestire l'invio del/i documento/i al ParER.
     *
     * @param context ?? il contesto che arriva dallo schedulatore
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("Versatore ParER Started");

        try (Connection dbConn = UtilityFunctions.getDBConnection();) {

            // controllo se ?? un verasmento "a volere" fuori dalla schedulazione
            ServiceRequestInformation serviceRequestInformation = getServiceRequestInformation(context);

            if (serviceRequestInformation.getModalitaAccesso() == ServiceRequestInformation.ModalitaAccesso.AMMINISTRAZIONE) {
                log.info("servizio versamento chiamato su richiesta in modalita amministrativa");
                doJob(context, serviceRequestInformation);
                log.info("Versatore ParER Finished");
            } else if (serviceRequestInformation.getModalitaAccesso() == ServiceRequestInformation.ModalitaAccesso.APPLICAZIONE) {
                log.info("servizio versamento chiamato su richiesta in modalita applicazione");
                doJob(context, serviceRequestInformation);
                log.info("Versatore ParER Finished");
            } else {
                // parte schedulata, controllo se il servizio ?? gi?? stato eseguito oggi
                log.info("servizio versamento schedulato");

                if (nonFattoOggi(dbConn)) {
                    // controllo che i servizi applicativi (attivi) che forniscono dati per l'invio al ParER sono terminati
                    // if (serviziApplicativiFiniti(dbConn)) {
                    updateDataInizioDataFine(dbConn, Timestamp.valueOf(LocalDateTime.now()), null);
                    doJob(context, serviceRequestInformation);
                    updateDataInizioDataFine(dbConn, null, Timestamp.valueOf(LocalDateTime.now()));
                    log.info("Versatore ParER Finished");
//                    } else {
//                        log.info("servizi calcolo idoneita applicazioni non tutti finiti");
//                        log.debug("Versatore ParER Finished");
//                    }
                } else {
                    log.info("servizio versatore ParER gi?? eseguito oggi");
                    log.info("Versatore ParER Finished");
                }
            }

//            if (versamentoRichiestoFuoriServizio(context)) {
//                log.info("servizio versamento chiamato a volere");
//                doJob(context);
//                log.info("Versatore ParER Finished");
//            } else {
//                // parte schedulata, controllo se il servizio ?? gi?? stato eseguito oggi
//                log.info("servizio versamento schedulato");
//
//                if (nonFattoOggi(dbConn)) {
//                    // controllo che i servizi applicativi (attivi) che forniscono dati per l'invio al ParER sono terminati
//                    if (serviziApplicativiFiniti(dbConn)) {
//                        updateDataInizioDataFine(dbConn, Timestamp.valueOf(LocalDateTime.now()), null);
//                        doJob(context);
//                        updateDataInizioDataFine(dbConn, null, Timestamp.valueOf(LocalDateTime.now()));
//                        log.info("Versatore ParER Finished");
//                    } else {
//                        log.info("servizi calcolo idoneita applicazioni non tutti finiti");
//                        log.debug("Versatore ParER Finished");
//                    }
//                } else {
//                    log.info("servizio versatore ParER gi?? eseguito oggi");
//                    log.debug("Versatore ParER Finished");
//                }
//            }
        } catch (Exception ex) {
            log.error("Errore nel servizio di versatore al ParER: ", ex);
            log.info("Versatore ParER Finished");
        }

    }

    /**
     * determina, in base al contesto passato, se il servizio ?? chiamato dallo
     * schedulatore oppure "a volere"
     *
     * @param context
     * @return true: se servizio ?? stato richiesto "a volere" false: se servizio
     * ?? stato richiesto dallo schedulatore
     */
    public boolean versamentoRichiestoFuoriServizio(JobExecutionContext context) {
        log.info("versamentoRichiestoFuoriServizio");
        String data = context.getJobDetail().getJobDataMap().getString(NOME_SERVIZIO);
        JSONObject parse = (JSONObject) JSONValue.parse(data);
        String idUtente = (String) parse.get("idUtente");
        List<String> documenti = (List<String>) parse.get("documenti");
        if (data != null && !data.equals("")) {
            log.info("data != null -> servizio chiamato a volere");
            return true;
        }
        log.info("data = null -> parte il servizio schedulato");
        return false;

    }

    /**
     * determina, in base al contesto passato, se il servizio ?? chiamato dallo
     * schedulatore, dalla pagina amministrativa di bds_tools oppure dal
     * cruscotto ParER su Babel
     *
     * @param context
     * @return ServiceRequestInformation: informazioni ottenute dalla richiesta
     */
    public ServiceRequestInformation getServiceRequestInformation(JobExecutionContext context) throws UnsupportedEncodingException {

        ServiceRequestInformation res = new ServiceRequestInformation();

        String data = context.getJobDetail().getJobDataMap().getString(NOME_SERVIZIO);

        if (data != null && !data.equals("")) {
            log.info("servizio chiamato su richiesta");
            String newData = URLDecoder.decode(data, "ISO-8859-1");
            JSONObject parse = (JSONObject) JSONValue.parse(newData);
            String idUtente = (String) parse.get("idUtente");
            String azione = (String) parse.get("azione");
            String motivazioneAzione = (String) parse.get("motivazioneAzione");

            if (idUtente != null && !idUtente.equals("")) {
                log.info("modalita richiesta: APPLICAZIONE");
                res.setModalitaAccesso(ServiceRequestInformation.ModalitaAccesso.APPLICAZIONE);
                res.setIdUtente(idUtente);

                if (azione != null && !azione.equals("") && motivazioneAzione != null && !motivazioneAzione.equals("")) {
                    res.setAzione(azione);
                    res.setMotivazioneAzione(motivazioneAzione);
                }
            } else {
                log.info("modalita richiesta: AMMINISTRAZIONE");
                res.setModalitaAccesso(ServiceRequestInformation.ModalitaAccesso.AMMINISTRAZIONE);
            }

            List<String> documenti = (List<String>) parse.get("documenti");
            res.setContent(documenti);
        } else {
            log.info("modalita richiesta: SERVIZIO_SCHEDULATO");
            res.setModalitaAccesso(ServiceRequestInformation.ModalitaAccesso.SCHEDULATO);
        }
        return res;
    }

    /**
     * reperisce i gddoc da versare, completa i dati mancanti e li invia al
     * ParER
     *
     * @param context
     */
    public void doJob(JobExecutionContext context, ServiceRequestInformation serviceRequestInformation) throws MasterChefClientException {

        // estrazione dei dati passati al servizio (se ci sono) se ?? stato richiesto un versamento "a volere"
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String content = dataMap.getString("VersatoreParer");

        VersatoreParerUtils versatoreParerUtils = new VersatoreParerUtils();

        log.info("contenuto come versamento - a volere - : " + content);

        // setta parametri di sessione versamento (id e guid di INDE)
        Map<String, String> idGuidINDE = setStartSessioneVersamento();
        String idSessioneVersamento = idGuidINDE.get(INDE_DOCUMENT_ID_PARAM_NAME);
        String guidSessioneVersamento = idGuidINDE.get(INDE_DOCUMENT_GUID_PARAM_NAME);

        // gddocs che possono essere versati
        ArrayList<String> gdDocList = null;

        try {

            if (serviceRequestInformation.getModalitaAccesso() == ServiceRequestInformation.ModalitaAccesso.SCHEDULATO) {
                // sono nella modalit?? schedulata
                gdDocList = getIdGdDocDaVersate();
            } else {
                // sono nella modalit?? "a volere"
                gdDocList = getIdFromGuid((ArrayList<String>) serviceRequestInformation.getContent());
            }

//            if (content != null && !content.equals("")) {
//                // sono nella modalit?? "a volere"
//                gdDocList = getIdFromGuid(getIdGdDocFromString(content));
//            } else {
//                // sono nella modalit?? schedulata
//                gdDocList = getIdGdDocDaVersate();
//            }
            // se la lista dei gddocs da versare non ?? nulla o vuota allora si procede a versarli
            if (gdDocList != null && gdDocList.size() > 0) {

                // verifica se si ha l'id di versameno di INDE da scrivere poi nella tabella 'gd.gddocs_sessioni_versamento'
                if (idSessioneVersamento != null && !idSessioneVersamento.equals("")) {

                    log.info("numero di gddoc da versare: " + gdDocList.size());
                    boolean isVersabile = false;

                    // reperimento dell'oggetto GdDoc vero e proprio (comprese le sue collection)
                    for (String idGdDoc : gdDocList) {

                        GdDoc gddoc = null;
                        String xmlVersato = null;

                        try {
                            gddoc = getGdDocById(idGdDoc);
                        } catch (Exception e) {
                            // se fallisce la costruzione del gddoc allora lo segno sugli errori versamento e salto al prossimo da versare
                            log.error("fallita ricostruzione del gddoc con ID: " + idGdDoc);
                            GdDocSessioneVersamento gdSessioneVersamento = new GdDocSessioneVersamento();
                            gdSessioneVersamento.setIdSessioneVersamento(idSessioneVersamento);
                            gdSessioneVersamento.setGuidSessioneVersamento(guidSessioneVersamento);
                            gdSessioneVersamento.setIdGdDoc(idGdDoc);
                            gdSessioneVersamento.setXmlVersato(null);
                            gdSessioneVersamento.setEsito("ERRORE");
                            // il valore di codice errore = 0 ?? stato scelto arbitrariamente perch?? diverso dagli errori che ritornano dal ParER
                            gdSessioneVersamento.setCodiceErrore(String.valueOf(0));
                            gdSessioneVersamento.setDescrizioneErrore("fallita ricostruzione del gddoc con errore: " + e);
                            gdSessioneVersamento.setAzione(serviceRequestInformation.getAzione());
                            gdSessioneVersamento.setMotivazioneAzione(serviceRequestInformation.getMotivazioneAzione());
                            saveVersamento(gdSessioneVersamento, idGdDoc);
                            continue;
                        }

                        try {
                            // determino se il gddoc ?? versabile oppure no
                            isVersabile = isVersabile(gddoc);

                            if (isVersabile) {
                                // ricarico gddoc
                                //gddoc = getGdDocById(idGdDoc);
                                // calcolo i dati
                                GdDocSessioneVersamento gdSessioneVersamento = new GdDocSessioneVersamento();
                                DatiParerGdDoc datiparer = gddoc.getDatiParerGdDoc();

                                gdSessioneVersamento.setIdSessioneVersamento(idSessioneVersamento);
                                gdSessioneVersamento.setGuidSessioneVersamento(guidSessioneVersamento);
                                gdSessioneVersamento.setIdGdDoc(idGdDoc);

                                try {
                                    SendToParerParams sendToParerParams = getSendToParerParams(gddoc);
                                    log.info("sendToParerParams creati");
                                    xmlVersato = sendToParerParams.getXmlDocument();
                                    String codiceErrore, descrizioneErrore, rapportoVersamento;

                                    gdSessioneVersamento.setXmlVersato(xmlVersato);

                                    gdSessioneVersamento.setAzione(serviceRequestInformation.getAzione());
                                    gdSessioneVersamento.setMotivazioneAzione(serviceRequestInformation.getMotivazioneAzione());

                                    // lancio del mestiere
                                    String retQueue = "notifica_errore" + "_" + ApplicationParams.getAppId() + "_sendToParerRetQueue_" + ApplicationParams.getServerId();
                                    log.info("Creazione Worker..");
                                    WorkerData wd = new WorkerData(ApplicationParams.getAppId(), "1", retQueue);
                                    log.info("Worker creato");
                                    wd.addNewJob("1", null, sendToParerParams);

                                    RedisClient redisClient = new RedisClient(ApplicationParams.getRedisHost(), null);
                                    log.info("Aggiungo Worker a redis..");
                                    redisClient.put(wd.getStringForRedis(), ApplicationParams.getRedisInQueue());
                                    log.info("Worker aggiunto su redis");

                                    String extractedValue = redisClient.bpop(retQueue, 86400);

                                    // se extractedValue = null ?? scaduto il timeout
                                    WorkerResponse workerResponse = new WorkerResponse(extractedValue);
                                    SendToParerErrorDetails castedErrorDetails = null;

                                    ErrorDetails errorDetail = workerResponse.getErrorDetails();

                                    if (errorDetail != null) {
                                        castedErrorDetails = (SendToParerErrorDetails) errorDetail;

                                        codiceErrore = castedErrorDetails.getErrorCode();
                                        descrizioneErrore = castedErrorDetails.getErrorMessage();
                                        rapportoVersamento = castedErrorDetails.getParerResponse();

                                        log.error("errore versamento id_gddoc: " + gddoc.getId()
                                                + " con codice: " + codiceErrore
                                                + " messaggio: " + descrizioneErrore);

                                        gdSessioneVersamento.setRapportoVersamento(rapportoVersamento);
                                        gdSessioneVersamento.setCodiceErrore(codiceErrore);
                                        if (codiceErrore.equals("SERVIZIO")) {
                                            gdSessioneVersamento.setDescrizioneErrore("Riscontrato un problema nel servizio. Il documento sar?? versato nella prossima sessione di versamento");
                                            gdSessioneVersamento.setEsito("SERVIZIO");

                                            datiparer.setStatoVersamentoProposto("da_versare");
                                            datiparer.setStatoVersamentoEffettivo("non_versare");
                                        } else {
                                            gdSessioneVersamento.setDescrizioneErrore(descrizioneErrore);
                                            gdSessioneVersamento.setEsito("ERRORE");

                                            String documentoInErrore = versatoreParerUtils.getNomeDocumentoInErrore(xmlVersato, descrizioneErrore);
                                            if (documentoInErrore != null && !documentoInErrore.equals("")) {
                                                gdSessioneVersamento.setDocumentoInErrore(documentoInErrore);
                                            }

                                            datiparer.setStatoVersamentoProposto("errore_versamento");
                                            datiparer.setStatoVersamentoEffettivo("errore_versamento");
                                        }
                                    }

                                    if (!workerResponse.getStatus().equals("OK")) {
                                        log.error("workerResponse.getStatus() != OK");
                                    } else {
                                        log.info("workerResponse.getStatus() == OK");
                                        SendToParerResult castedResult = null;
                                        WorkerResult workerResult = workerResponse.getResult(0);

                                        if (workerResult.getRes() != null) {
                                            castedResult = (SendToParerResult) workerResult.getRes();
                                            gdSessioneVersamento.setRapportoVersamento(castedResult.getParerResponse());
                                        }

                                        gdSessioneVersamento.setEsito("OK");
                                        gdSessioneVersamento.setCodiceErrore(null);
                                        gdSessioneVersamento.setDescrizioneErrore(null);
                                        gdSessioneVersamento.setAzione(serviceRequestInformation.getAzione());
                                        gdSessioneVersamento.setMotivazioneAzione(serviceRequestInformation.getMotivazioneAzione());

                                        datiparer.setStatoVersamentoProposto("versato");
                                        datiparer.setStatoVersamentoEffettivo("versato");
                                    }

                                    datiparer.setEsitoUltimoVersamento(gdSessioneVersamento.getEsito());
                                    datiparer.setCodiceErroreUltimoVersamento(gdSessioneVersamento.getCodiceErrore());

                                    // non serve pi?? sta parte. Mi calcolo lo stato quando estraggo i dati basandomi sul codice_errore e l'esito
//                                    // mi setto lo stato dell'ultimo versamento. I valori sono: OK, ERRORE_FORZABILE, ERRORE_NON_FORZABILE
//                                    if (datiparer.getEsitoUltimoVersamento().equals("ERRORE") && datiparer.getCodiceErroreUltimoVersamento().equals("UD-008-001")
//                                            || (!datiparer.getCodiceErroreUltimoVersamento().equals("FIRMA-005-001") && datiparer.getCodiceErroreUltimoVersamento().startsWith("FIRMA"))) {
//                                        datiparer.setStatoUltimoVersamento("ERRORE_FORZABILE");
//                                    } else if (datiparer.getEsitoUltimoVersamento().equals("ERRORE")
//                                            && (datiparer.getCodiceErroreUltimoVersamento().equals("UD-008-001")
//                                                || !datiparer.getCodiceErroreUltimoVersamento().equals("UD-008-001")
//                                                    && (!datiparer.getCodiceErroreUltimoVersamento().startsWith("FIRMA") || datiparer.getCodiceErroreUltimoVersamento().equals("FIRMA-005-001"))
//                                                || !datiparer.getCodiceErroreUltimoVersamento().equals("FIRMA-005-001") && datiparer.getCodiceErroreUltimoVersamento().startsWith("FIRMA")
//                                                )
//                                            ){
//                                        datiparer.setStatoUltimoVersamento("ERRORE_NON_FORZABILE");
//                                    } else if ( datiparer.getEsitoUltimoVersamento().equals("OK")){
//                                        datiparer.setStatoUltimoVersamento("OK");
//                                    }
//
                                    saveVersamentoAndGdDocInTransaction(gdSessioneVersamento, gddoc, datiparer, xmlVersato);
                                } catch (Exception e) {
                                    log.error("errore nel versamento: " + e);

                                    // imposto gli stati di errore
                                    gdSessioneVersamento.setEsito("ERRORE");
                                    gdSessioneVersamento.setAzione(serviceRequestInformation.getAzione());
                                    gdSessioneVersamento.setMotivazioneAzione(serviceRequestInformation.getMotivazioneAzione());
//                                    gdSessioneVersamento.setCodiceErrore(String.valueOf(0));
//                                    gdSessioneVersamento.setDescrizioneErrore(e.toString());

                                    datiparer.setEsitoUltimoVersamento(gdSessioneVersamento.getEsito());
                                    datiparer.setCodiceErroreUltimoVersamento(gdSessioneVersamento.getCodiceErrore());

                                    datiparer.setStatoVersamentoProposto("errore_versamento");
                                    datiparer.setStatoVersamentoEffettivo("errore_versamento");

                                    // persist
                                    saveVersamentoAndGdDocInTransaction(gdSessioneVersamento, gddoc, datiparer, xmlVersato);
                                }
                            }
                        } catch (Exception ex) {
                            log.error("errore nel versamento con idGdDoc: " + gddoc.getId() + " eccezione: " + ex);

                            GdDocSessioneVersamento gdSessioneVersamento = new GdDocSessioneVersamento();
                            DatiParerGdDoc datiparer = gddoc.getDatiParerGdDoc();

                            // imposto gli stati di errore
                            gdSessioneVersamento.setIdSessioneVersamento(idSessioneVersamento);
                            gdSessioneVersamento.setGuidSessioneVersamento(guidSessioneVersamento);
                            gdSessioneVersamento.setIdGdDoc(gddoc.getId());

                            gdSessioneVersamento.setEsito("ERRORE");
                            gdSessioneVersamento.setCodiceErrore(String.valueOf(0));
                            gdSessioneVersamento.setDescrizioneErrore(ex.getMessage());
                            datiparer.setStatoVersamentoProposto("errore_versamento");
                            datiparer.setStatoVersamentoEffettivo("errore_versamento");

                            // persist
                            saveVersamentoAndGdDocInTransaction(gdSessioneVersamento, gddoc, datiparer, xmlVersato);
                        }
                    }
                }
                //setStopSessioneVersamento(idSessioneVersamento);
                log.info("fine sessione di versamento");
            }
        } catch (Throwable t) {
            log.fatal("Versatore Parer: Errore ...", t);
        } finally {
            setStopSessioneVersamento(idSessioneVersamento, gdDocList, serviceRequestInformation);

            log.info("Job Versatore Parer finished");
            log.info("Versatore ParER Finished");
        }
    }

    /**
     * estrae gli igGdDoc dalla stringa passata
     *
     * @param str
     * @return array di idGdDoc
     */
    private ArrayList getIdGdDocFromString(String str) {
        JSONArray jsonResultArray = (JSONArray) JSONValue.parse(str);
        ArrayList<String> result = new ArrayList<>();
        jsonResultArray.stream().forEach((object) -> {
            result.add((String) object);
        });
        return result;
    }

    /**
     * crea una stringa da inserire nella query per filtrare le tipologie
     * documento ammesse al versamento
     *
     * @return la stringa da inserire nella stringa sql
     */
    private String getCodiciAbilitati() {
        String res = "";
        ArrayList<String> codici = new ArrayList<>();

        if (getCanSendDeli()) {
            codici.add("'DELI'");
        }

        if (getCanSendDete()) {
            codici.add("'DETE'");
        }

        if (getCanSendPicoEntrata() || getCanSendPicoUscita()) {
            codici.add("'PG'");
        }

        if (getCanSendRgDeli()) {
            codici.add("'RGDELI'");
        }

        if (getCanSendRgPico()) {
            codici.add("'RGPICO'");
        }

        if (getCanSendRgDete()) {
            codici.add("'RGDETE'");
        }

        if (codici.size() > 0) {
            res = codici.get(0);
            for (int i = 1; i < codici.size(); i++) {
                res = res + "," + codici.get(i);
            }
        } else {
            res = "''";
        }

        return res;
    }

    /**
     * Estrae la lista dei GdDoc che possono essere versati nella sessione
     * corrente al ParER
     *
     * @returnarray degli idGdDoc dei gddoc da versare
     * @throws SQLException
     * @throws NamingException
     * @throws VersatoreParerException
     */
    private ArrayList<String> getIdGdDocDaVersate() throws SQLException, NamingException, VersatoreParerException {

        String query = null;
        ArrayList<String> res = new ArrayList<>();

        if (getLimit() > 0) {
            query
                    = "SELECT dp.id_gddoc "
                    + "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " dp, " + ApplicationParams.getGdDocsTableName() + " g "
                    + "WHERE dp.id_gddoc = g.id_gddoc "
                    + "AND (dp.stato_versamento_effettivo = 'non_versare' or dp.stato_versamento_effettivo = 'errore_versamento') "
                    + "AND dp.xml_specifico_parer IS NOT NULL and dp.xml_specifico_parer !=  '' "
                    + "AND (dp.stato_versamento_proposto = 'da_versare' or dp.stato_versamento_proposto = 'da_aggiornare') "
                    + "AND g.codice_registro in (" + getCodiciAbilitati() + ") "
                    + "AND dp.idoneo_versamento != 0 "
                    + "LIMIT " + getLimit();
        } else if (!getDateFrom().equals("") && getDateTo().equals("")) {
            query
                    = "SELECT dp.id_gddoc "
                    + "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " dp, " + ApplicationParams.getGdDocsTableName() + " g "
                    + "WHERE (dp.stato_versamento_effettivo = 'non_versare' or dp.stato_versamento_effettivo = 'errore_versamento') "
                    + "AND dp.xml_specifico_parer IS NOT NULL and dp.xml_specifico_parer !=  '' "
                    + "AND (dp.stato_versamento_proposto = 'da_versare' or dp.stato_versamento_proposto = 'da_aggiornare') "
                    + "AND g.id_gddoc = dp.id_gddoc "
                    + "AND dp.idoneo_versamento != 0 "
                    + "AND g.codice_registro in (" + getCodiciAbilitati() + ") "
                    + "AND g.data_registrazione > '" + getDateFrom() + "' ";
        } else if (!getDateFrom().equals("") && !getDateTo().equals("")) {
            query
                    = "SELECT dp.id_gddoc "
                    + "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " dp, " + ApplicationParams.getGdDocsTableName() + " g "
                    + "WHERE (dp.stato_versamento_effettivo = 'non_versare' or dp.stato_versamento_effettivo = 'errore_versamento') "
                    + "AND dp.xml_specifico_parer IS NOT NULL and dp.xml_specifico_parer !=  '' "
                    + "AND (dp.stato_versamento_proposto = 'da_versare' or dp.stato_versamento_proposto = 'da_aggiornare') "
                    + "AND g.id_gddoc = dp.id_gddoc "
                    + "AND dp.idoneo_versamento != 0 "
                    + "AND g.codice_registro in (" + getCodiciAbilitati() + ") "
                    + "AND g.data_registrazione > '" + getDateFrom() + "' "
                    + "AND g.data_registrazione < '" + getDateTo() + "' ";
        } else {
            query
                    = "SELECT dp.id_gddoc "
                    + "FROM " + ApplicationParams.getDatiParerGdDocTableName() + " dp, " + ApplicationParams.getGdDocsTableName() + " g "
                    + "WHERE dp.id_gddoc = g.id_gddoc "
                    + "AND (dp.stato_versamento_effettivo = 'non_versare' or dp.stato_versamento_effettivo = 'errore_versamento') "
                    + "AND dp.xml_specifico_parer IS NOT NULL and dp.xml_specifico_parer !=  '' "
                    + "AND (dp.stato_versamento_proposto = 'da_versare' or dp.stato_versamento_proposto = 'da_aggiornare') "
                    + "AND dp.idoneo_versamento != 0 "
                    + "AND g.codice_registro in (" + getCodiciAbilitati() + ") ";
        }

        log.info("eseguo la query: " + query);

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query)) {
            if (dbConnection != null) {
                log.info("dbConnection is not null");
            } else {
                log.info("dbConnction == null");
            }

            log.info("ottengo id dei gddoc da versare");

            log.info("PrepareStatment: " + ps);
            ResultSet resultQuery = ps.executeQuery();

            while (resultQuery.next()) {
                res.add(resultQuery.getString("id_gddoc"));
            }
        } catch (Exception ex) {
            throw new VersatoreParerException("Errore nel reperimento dei gddoc da inviare", ex);
        }
        return res;
    }

    /**
     * Salva i dati parer gddoc e inserisce il record di versamento effettuato
     * nella tabella 'gd.gddocs_sessioni_versamento'
     *
     * @param g
     * @param gddoc
     * @param datiParer
     */
    private void saveVersamentoAndGdDocInTransaction(GdDocSessioneVersamento g, GdDoc gddoc, DatiParerGdDoc datiParer, String xmlVersato) {

        String query
                = "INSERT INTO " + ApplicationParams.getGdDocSessioniVersamentoParerTableName() + "( "
                + "id_gddoc_versamento, id_gddoc, id_sessione_versamento_parer, "
                + "xml_versato, esito, codice_errore, descrizione_errore, "
                + "rapporto_versamento, guid_gddoc_versamento, documento_in_errore, azione, motivazione_azione) "
                + "VALUES (?, ?, ?, "
                + "?, ?, ?, ?, "
                + "?, ?, ?, ?, ?)";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query)) {
            dbConnection.setAutoCommit(false);
            log.info("salvataggio di un gddoc_sessione_versamento");
            // ottengo in ID di INDE
            String idInde = getIndeId().get(INDE_DOCUMENT_ID_PARAM_NAME);
            String guidInde = getIndeId().get(INDE_DOCUMENT_GUID_PARAM_NAME);
            log.info("valore id INDE: " + idInde);

            int index = 1;

            // ID di INDE
            ps.setString(index++, idInde);
            ps.setString(index++, gddoc.getId());
            ps.setString(index++, g.getIdSessioneVersamento());
            ps.setString(index++, g.getXmlVersato());
            ps.setString(index++, g.getEsito());
            ps.setString(index++, g.getCodiceErrore());
            ps.setString(index++, g.getDescrizioneErrore());
            ps.setString(index++, g.getRapportoVersamento());
            ps.setString(index++, guidInde);
            ps.setString(index++, g.getDocumentoInErrore());
            ps.setString(index++, g.getAzione());
            ps.setString(index++, g.getMotivazioneAzione());

            int rowsUpdated = ps.executeUpdate();
            log.info("eseguita");
            if (rowsUpdated == 0) {
                dbConnection.rollback();
                throw new SQLException("record di gddoc_sessione_versamento non inserito");
            }

            updateDatiParerGdDoc(datiParer, gddoc, xmlVersato, g.getDescrizioneErrore());

            dbConnection.commit();

        } catch (SQLException | NamingException | IOException | SendHttpMessageException ex) {
            log.fatal("errore: " + ex);
        }
    }

    /**
     * Inserisce il record nella tabella 'gd.gddocs_sessioni_versamento'
     *
     * @param g
     * @param idGdDoc
     */
    private void saveVersamento(GdDocSessioneVersamento g, String idGdDoc) {

        String query
                = "INSERT INTO " + ApplicationParams.getGdDocSessioniVersamentoParerTableName() + "( "
                + "id_gddoc_versamento, id_gddoc, id_sessione_versamento_parer, "
                + "xml_versato, esito, codice_errore, descrizione_errore, "
                + "rapporto_versamento, guid_gddoc_versamento, documento_in_errore) "
                + "VALUES (?, ?, ?, "
                + "?, ?, ?, ?, "
                + "?, ?, ?)";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query)) {
            dbConnection.setAutoCommit(false);
            log.info("salvataggio di un gddoc_sessione_versamento");
            // ottengo in ID di INDE
            String idInde = getIndeId().get(INDE_DOCUMENT_ID_PARAM_NAME);
            String guidInde = getIndeId().get(INDE_DOCUMENT_GUID_PARAM_NAME);
            log.info("valore id INDE: " + idInde);

            int index = 1;

            // ID di INDE
            ps.setString(index++, idInde);
            ps.setString(index++, idGdDoc);
            ps.setString(index++, g.getIdSessioneVersamento());
            ps.setString(index++, g.getXmlVersato());
            ps.setString(index++, g.getEsito());
            ps.setString(index++, g.getCodiceErrore());
            ps.setString(index++, g.getDescrizioneErrore());
            ps.setString(index++, g.getRapportoVersamento());
            ps.setString(index++, guidInde);
            ps.setString(index++, g.getDocumentoInErrore());

            //log.debug("PrepareStatment: " + ps);
            int rowsUpdated = ps.executeUpdate();
            log.info("eseguita");
            if (rowsUpdated == 0) {
                dbConnection.rollback();
                throw new SQLException("record di gddoc_sessione_versamento non inserito");
            }
            dbConnection.commit();
        } catch (SQLException | NamingException | IOException | SendHttpMessageException ex) {
            log.fatal("errore: " + ex);
        }
    }

    /**
     * imposta l'inizio della sessione versamento
     *
     * @return id e guid INDE della sessione di versamento appena iniziata
     */
    private Map<String, String> setStartSessioneVersamento() {

        Map<String, String> res = null;

        String query
                = "INSERT INTO " + ApplicationParams.getSessioniVersamentoParerTableName() + "( "
                + "id_sessione_versamento_parer, data_inizio, guid_sessione_versamento_parer) "
                + "VALUES (?, date_trunc('sec', now()::timestamp), ?) ";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query)) {
            log.info("setto data e ora inizio sessione di versamento");

            // ottengo in ID di INDE
            String idInde = getIndeId().get(INDE_DOCUMENT_ID_PARAM_NAME);
            String guidInde = getIndeId().get(INDE_DOCUMENT_GUID_PARAM_NAME);
            log.info("valore id INDE: " + idInde);

            int index = 1;

            // ID di INDE
            ps.setString(index++, idInde);
            // GUID di INDE
            ps.setString(index++, guidInde);

            log.info("PrepareStatment: " + ps);
            int rowsUpdated = ps.executeUpdate();
            log.info("eseguita");
            if (rowsUpdated == 0) {
                throw new SQLException("data inizio non inserita");
            } else {
                res = new HashMap<>();
            }
            res.put(INDE_DOCUMENT_ID_PARAM_NAME, idInde);
            res.put(INDE_DOCUMENT_GUID_PARAM_NAME, guidInde);
        } catch (SQLException | NamingException | IOException | SendHttpMessageException ex) {
            log.fatal("errore inserimento data_inizio nella tabella sessioni_versamento_parer");
        }
        return res;
    }

    /**
     * imposta la fine della sessione versamento
     *
     * @param idSessioneVersamento
     */
    private void setStopSessioneVersamento(String idSessioneVersamento, List<String> gddocList, ServiceRequestInformation serviceRequestInformation) throws MasterChefClientException {

        //String pattern = "yyyy-MM-dd'T'HH:mm:ss";
        DateTime dateTime = DateTime.now();
        String nowString = DatiParerGdDoc.toIsoDateFormatString(dateTime, DATE_PATTERN_STANDARD);

        String query
                = //            "UPDATE " + ApplicationParams.getSessioniVersamentoParerTableName() + " " +
                //            "SET data_fine= date_trunc('sec', now()::timestamp) " +
                //            "WHERE id_sessione_versamento_parer = ? ";
                "UPDATE " + ApplicationParams.getSessioniVersamentoParerTableName() + " "
                + "SET data_fine = ?::timestamp without time zone "
                + "WHERE id_sessione_versamento_parer = ? ";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(query)) {
            int index = 1;
            log.info("setto ora fine sessione di versamento");

            ps.setString(index++, nowString);
            ps.setString(index++, idSessioneVersamento);

            log.info("eseguo la query: " + ps.toString() + " ...");
            int result = ps.executeUpdate();
            if (result <= 0) {
                throw new SQLException("Errore inserimento data fine sessione versamento ParER");
            }
            if (result == 0) {
                throw new SQLException("data fine non inserita");
            }
        } catch (SQLException | NamingException ex) {
            log.fatal("errore inserimento data_fine nella tabella sessioni_versamento_parer: " + ex);
        }

        // aggiornamento dati parer gddoc con set data fine versamento
//        String listString = "'" + String.join("', '", gddocList) + "'";
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < gddocList.size(); i++) {
            builder.append("?,");
        }

        if (builder.length() > 0) {
            String queryUpdateDatiParer = "UPDATE " + ApplicationParams.getDatiParerGdDocTableName() + " "
                    + "SET data_ultimo_versamento = ?::timestamp without time zone "
                    + "WHERE id_gddoc in (" + builder.deleteCharAt(builder.length() - 1).toString() + ") ";

            try (
                    Connection dbConnection = UtilityFunctions.getDBConnection();
                    PreparedStatement ps = dbConnection.prepareStatement(queryUpdateDatiParer)) {
                int index = 1;
                log.info("setto ora fine sessione di versamento");

                ps.setString(index++, nowString);

                for (String o : gddocList) {
                    ps.setString(index++, o);
                }

//ps.setString(index++, listString);
                log.info("eseguo la query: " + ps.toString() + " ...");
                int result = ps.executeUpdate();
                if (result <= 0) {
                    throw new SQLException("Errore inserimento data_ultimo_versamento in dati_parer_gddoc");
                }
                if (result == 0) {
                    throw new SQLException("data_ultimo_versamento fine non inserita");
                }
            } catch (SQLException | NamingException ex) {
                log.fatal("errore inserimento data_ultimo_versamento nella tabella dati_parer_gddoc: " + ex);
            }
        }

        riversaErroriInTabellaCheck(nowString);

//        if (serviceRequestInformation.getModalitaAccesso() == ServiceRequestInformation.ModalitaAccesso.APPLICAZIONE){
//            inviaNotificaSuScrivania(serviceRequestInformation, idSessioneVersamento, nowString);
//        }
    }

    /**
     * A partire dal GdDoc costruisce i Params per il mestiere che si occuper??
     * di fare l'invio al ParER del documento
     *
     * @param gdDoc
     * @return oggetto Params da passare al mestiere
     * @throws UnsupportedEncodingException
     * @throws SQLException
     * @throws NamingException
     * @throws DatatypeConfigurationException
     * @throws JAXBException
     * @throws ParseException
     */
    private SendToParerParams getSendToParerParams(GdDoc gdDoc) throws UnsupportedEncodingException, SQLException, NamingException, DatatypeConfigurationException, JAXBException, ParseException {
        log.info("inizio funzione getSendToParerParams");

        SendToParerParams res = null;

        String idUnitaDocumentaria, tipoDocumento, dataDocumento;
        String forzaCollegamento, forzaAccettazione, forzaConservazione;
        int ordinePresentazione = 1;
        DatiSpecifici datiSpecifici;
        ProfiloArchivistico profiloArchivistico;

        // determina se usare id fake dell'unit?? documentale oppure i reali
        if (getUseFakeId()) {
            // utilizza un numero FAKE
            log.info("utilizzo di id fake");
            Random randomGenerator = new Random();
            idUnitaDocumentaria = String.valueOf(randomGenerator.nextInt(100000000));
        } else {
            // utilizza il numero di protocollazione REALE
            log.info("utilizzo di id reale");
            idUnitaDocumentaria = gdDoc.getNumeroRegistrazione();
        }
        log.info("idUnitaDocumentaria: " + idUnitaDocumentaria);

        // ottiene xml specifico vero e proprio
        datiSpecifici = getDatiSpecifici(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "xmlpart");
        log.info("datiSpecifici settati");

        // setta il tipo documento in base alla movimentazione
        tipoDocumento = getTipoDocumentoUnitaDocumentaria(gdDoc, "movimentazione");
        log.info("tipoDocumento settato: " + tipoDocumento);

        // estrae la data del documento
        dataDocumento = getStringDateParer(gdDoc.getDataRegistrazione());
        log.info("dataDocumento impostata: " + dataDocumento);

        // imposta le forzature di versamento
        forzaConservazione = getStringBoolean(gdDoc.getDatiParerGdDoc().getForzaConservazione());
        forzaAccettazione = getStringBoolean(gdDoc.getDatiParerGdDoc().getForzaAccettazione());
        forzaCollegamento = getStringBoolean(gdDoc.getDatiParerGdDoc().getForzaCollegamento());

        // crea il profilo archivistico
        profiloArchivistico = createProfiloArchivistico(gdDoc);
        log.info("profiloArchivistico creato");

        // ottieni gli array degli IdentityFile di Allegati, Annessi, Annotazioni
        ArrayList<IdentityFile> arrayIdentityFileAllegati = getArrayIdentityFileFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "allegatidentityfile");
        ArrayList<IdentityFile> arrayIdentityFileAnnessi = getArrayIdentityFileFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "annessidentityfile");
        ArrayList<IdentityFile> arrayIdentityFileAnnotazioni = getArrayIdentityFileFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "annotazionidentityfile");

        // identity file del documento principale
        IdentityFile ifDocPrincipale = getIdentityFileFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "principalidentityfile");

        // calcola il tipo del documento principale
        String tipoDocumentoPrincipale = getTipoDocumentoUnitaDocumentaria(gdDoc, "applicazione");
        log.info("tipoDocumentoPrincipale: " + tipoDocumentoPrincipale);

        // calcola il responsabile procedimento
        String autore = getStringFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "responsabile_procedimento");
        log.info("responsabile procedimento: " + autore);

        // calcola i firmatari e li inserisce come autori, se ci sono
        String autoreProfiloDocumento = getStringFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "firmatari");
        if (autoreProfiloDocumento == null || autoreProfiloDocumento.equals("")) {
            autoreProfiloDocumento = "";
        }

        // ottieni informazioni sul documento principale
        JSONObject infoDocPrincipale = getJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "infodocprincipale");

        // ottieni informazioni sui documenti secondari
        ArrayList<InfoDocumento> infoAllegati = getArrayInfoDocumentoFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "informazioniallegati");
        ArrayList<InfoDocumento> infoAnnessi = getArrayInfoDocumentoFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "informazioniannessi");
        ArrayList<InfoDocumento> infoAnnotazioni = getArrayInfoDocumentoFromJson(gdDoc.getDatiParerGdDoc().getXmlSpecifico(), "informazioniannotazioni");

        // creazione unit?? documentaria
        UnitaDocumentariaBuilder unitaDocumentaria = new UnitaDocumentariaBuilder(idUnitaDocumentaria,
                gdDoc.getAnnoRegistrazione(),
                gdDoc.getCodiceRegistro(),
                tipoDocumento,
                forzaConservazione,
                forzaAccettazione,
                forzaCollegamento,
                profiloArchivistico,
                gdDoc.getOggetto(),
                dataDocumento,
                datiSpecifici,
                String.valueOf(getVersione()),
                getAmbiente(),
                getEnte(),
                getStrutturaVersante(),
                getUserID(),
                "VERSAMENTO_ANTICIPATO",
                getCodifica());

        log.info("unita documentaria creata");

        PubblicazioneIoda pubblicazioneEsecutiva = getPrimaPubblicazione(gdDoc.getPubblicazioni(), false);
        if (tipoDocumento.equals("DELIBERAZIONE")) {
//            Si ?? scelto di prendere DATA_DAL e non DATA_ESECUTIVITA perch?? si era concordato che la data di riferimento temporale
//            per la delibera ?? la data della prima pubblicazione (indipendentemente dalla sua esecutivit??)
            dataDocumento = getStringDateParer(pubblicazioneEsecutiva.getDataDal());
        }

        unitaDocumentaria.addDocumentoPrincipale(gdDoc.getIdOggettoOrigine(),
                tipoDocumentoPrincipale,
                autoreProfiloDocumento,
                (String) infoDocPrincipale.get("descrizione"),
                ordinePresentazione,
                ifDocPrincipale,
                dataDocumento,
                getTipoComponenteDefault(),
                "DocumentoGenerico",
                "FILE",
                getDescrizioneRiferimentoTemporale(tipoDocumento));

        log.info("aggiunto documento principale a unitaDocumentaria");

        log.info("inizio ciclo per aggiunta degli allegati");
        // aggiunta degli allegati
        if (arrayIdentityFileAllegati != null) {
            for (int i = 0; i < arrayIdentityFileAllegati.size(); i++) {
                ordinePresentazione++;

                // prendo informazione allegato
                InfoDocumento infoAllegato = (InfoDocumento) infoAllegati.get(i);

                IdentityFile ifAllegato = (IdentityFile) arrayIdentityFileAllegati.get(i);

                unitaDocumentaria.addDocumentoSecondario(infoAllegato.getUuidMongo(),
                        infoAllegato.getTipo(),
                        infoAllegato.getAutore(),
                        infoAllegato.getDescrizione(),
                        ordinePresentazione,
                        ifAllegato,
                        infoAllegato.getTipoStruttura(),
                        infoAllegato.getTipoDocumentoSecondario(),
                        "Contenuto",
                        "FILE",
                        dataDocumento,
                        getDescrizioneRiferimentoTemporale(tipoDocumento));

                log.info("allegato con ordine " + ordinePresentazione + "inserito con guid: " + infoAllegato.getGuid());
            }
        }
        log.info("fine ciclo per aggiunta degli allegati");

        log.info("inizio ciclo per aggiunta degli annessi");
        // aggiunta degli annessi
        if (arrayIdentityFileAnnessi != null) {
            for (int i = 0; i < arrayIdentityFileAnnessi.size(); i++) {
                ordinePresentazione++;

                // prendo informazione allegato
                InfoDocumento infoAnnesso = (InfoDocumento) infoAnnessi.get(i);

                IdentityFile ifAnnesso = (IdentityFile) arrayIdentityFileAnnessi.get(i);

                unitaDocumentaria.addDocumentoSecondario(infoAnnesso.getUuidMongo(),
                        infoAnnesso.getTipo(),
                        infoAnnesso.getAutore(),
                        infoAnnesso.getDescrizione(),
                        ordinePresentazione,
                        ifAnnesso,
                        infoAnnesso.getTipoStruttura(),
                        infoAnnesso.getTipoDocumentoSecondario(),
                        "Contenuto",
                        "FILE",
                        dataDocumento,
                        getDescrizioneRiferimentoTemporale(tipoDocumento));

                log.info("annesso con ordine " + ordinePresentazione + "inserito con guid: " + infoAnnesso.getGuid());
            }
        }
        log.info("fine ciclo per aggiunta degli annessi");

        log.info("inizio ciclo per aggiunta delle annotazioni");
        // aggiunta degli annotazioni
        if (arrayIdentityFileAnnotazioni != null) {
            for (int i = 0; i < arrayIdentityFileAnnotazioni.size(); i++) {
                ordinePresentazione++;

                // prendo informazione allegato
                InfoDocumento infoAnnotazione = (InfoDocumento) infoAnnotazioni.get(i);

                IdentityFile ifAnnotazione = (IdentityFile) arrayIdentityFileAnnotazioni.get(i);

                unitaDocumentaria.addDocumentoSecondario(infoAnnotazione.getUuidMongo(),
                        infoAnnotazione.getTipo(),
                        infoAnnotazione.getAutore(),
                        infoAnnotazione.getDescrizione(),
                        ordinePresentazione,
                        ifAnnotazione,
                        infoAnnotazione.getTipoStruttura(),
                        infoAnnotazione.getTipoDocumentoSecondario(),
                        "Contenuto",
                        "FILE",
                        null,
                        null);

                log.info("annotazione con ordine " + ordinePresentazione + "inserita con guid: " + infoAnnotazione.getGuid());
            }
        }
        log.info("fine ciclo per aggiunta delle annotazioni");

        try {
            // inizializzo oggetto di ritorno
            res = new SendToParerParams(unitaDocumentaria.toString(), "insert");
            log.info("sendToParerParams creato");

            // inserisco i riferimenti ai file
            JSONObject jsonIfDocPrincipale = ifDocPrincipale.getJSON();
            res.addIdentityFile(jsonIfDocPrincipale);

            if (arrayIdentityFileAllegati != null) {
                log.info("inizio ciclo arrayIndentityFileAllegati per inserire i riferimenti ai file");
                for (int i = 0; i < arrayIdentityFileAllegati.size(); i++) {
                    IdentityFile ifTmp = (IdentityFile) arrayIdentityFileAllegati.get(i);
                    JSONObject jsonTmp = ifTmp.getJSON();
                    res.addIdentityFile(jsonTmp);
                }
                log.info("fine ciclo");
            }

            if (arrayIdentityFileAnnessi != null) {
                log.info("inizio ciclo arrayIdentityFileAnnessi per inserire i riferimenti ai file");
                for (int i = 0; i < arrayIdentityFileAnnessi.size(); i++) {
                    IdentityFile ifTmp = (IdentityFile) arrayIdentityFileAnnessi.get(i);
                    JSONObject jsonTmp = ifTmp.getJSON();
                    res.addIdentityFile(jsonTmp);
                }
                log.info("fine ciclo");
            }

            if (arrayIdentityFileAnnotazioni != null) {
                log.info("inizio ciclo arrayIdentityFileAnnotazioni per inserire i riferimenti ai file");
                for (int i = 0; i < arrayIdentityFileAnnotazioni.size(); i++) {
                    IdentityFile ifTmp = (IdentityFile) arrayIdentityFileAnnotazioni.get(i);
                    JSONObject jsonTmp = ifTmp.getJSON();
                    res.addIdentityFile(jsonTmp);
                }
                log.info("fine ciclo");
            }
        } catch (Exception e) {
            log.error(e);
            log.info("setto res = null");
            res = null;
        }

        log.info("fine funzione getSendToParerParams");
        return res;
    }

    /**
     * Creazione del profilo archivistico a partire dal GdDoc
     *
     * @param gdDoc
     * @return ProfiloArchivistico calcolato
     * @throws SQLException
     * @throws NamingException
     */
    private ProfiloArchivistico createProfiloArchivistico(GdDoc gdDoc) throws SQLException, NamingException {
        log.info("creazione del profilo archivistico");

        ProfiloArchivistico res = null;

        // array dei titoli globali
        ArrayList<String> titoliFascicoli = new ArrayList<>();

        // prendo la prima fascicolazione inserita nel documento
        List<Fascicolazione> fascicolazioniOrdinate = ordinaFascicolazioni(gdDoc.getFascicolazioni());
        Fascicolazione primaFascicolazione = fascicolazioniOrdinate.get(0);

        try (Connection dbConnection = UtilityFunctions.getDBConnection()) {
            IodaFascicolazioniUtilities iodafu = new IodaFascicolazioniUtilities(null, null);
            // aggiungo il fascicolo prncipale al profilo archivistico
            BagProfiloArchivistico bag = iodafu.getGerarchiaFascicolo(dbConnection, primaFascicolazione.getCodiceFascicolo());

            if (bag != null) {
                res = new ProfiloArchivistico();
                switch (bag.getLevel()) {
                    case 1:
                        res.addFascicoloPrincipale(
                                getClassificaParerByString(bag.getClassificaFascicolo()), // classificazione
                                String.valueOf(bag.getAnnoFascicolo()), // anno data creazione fascicolo
                                String.valueOf(bag.getNumeroFasciolo()), // numero del fascicolo
                                bag.getNomeFascicolo(), // nome del fascicolo
                                "", // numero sotto-fascicolo
                                "", // nome sotto-fascicolo
                                "", // numero inserto
                                "");                                                         // nome inserto
                        titoliFascicoli.add(bag.getClassificaFascicolo().replaceAll("/", "-"));
                        break;

                    case 2:
                        res.addFascicoloPrincipale(
                                getClassificaParerByString(bag.getClassificaFascicolo()), // classificazione
                                String.valueOf(bag.getAnnoFascicolo()), // anno data creazione fascicolo
                                String.valueOf(bag.getNumeroFasciolo()), // numero del fascicolo
                                bag.getNomeFascicolo(), // nome del fascicolo
                                String.valueOf(bag.getNumeroSottoFascicolo()), // numero sotto-fascicolo
                                bag.getNomeSottoFascicolo(), // nome sotto-fascicolo
                                "", // numero inserto
                                "");                                                         // nome inserto
                        titoliFascicoli.add(bag.getClassificaFascicolo().replaceAll("/", "-"));
                        break;

                    case 3:
                        res.addFascicoloPrincipale(
                                getClassificaParerByString(bag.getClassificaFascicolo()), // classificazione
                                String.valueOf(bag.getAnnoFascicolo()), // anno data creazione fascicolo
                                String.valueOf(bag.getNumeroFasciolo()), // numero del fascicolo
                                bag.getNomeFascicolo(), // nome del fascicolo
                                String.valueOf(bag.getNumeroSottoFascicolo()), // numero sotto-fascicolo
                                bag.getNomeSottoFascicolo(), // nome sotto-fascicolo
                                String.valueOf(bag.getNumeroInserto()), // numero inserto
                                bag.getNomeInserto());                                       // nome inserto
                        titoliFascicoli.add(bag.getClassificaFascicolo().replaceAll("/", "-"));
                        break;
                }
            }

            // aggiungo i fascicoli secondari al profilo archivistico
            if (fascicolazioniOrdinate.size() > 1) {

                for (int i = 1; i < fascicolazioniOrdinate.size(); i++) {
                    Fascicolazione tmpFascicolazione = fascicolazioniOrdinate.get(i);
                    iodafu = new IodaFascicolazioniUtilities(null, null);

                    BagProfiloArchivistico tmpBag = iodafu.getGerarchiaFascicolo(dbConnection, tmpFascicolazione.getCodiceFascicolo());

                    if (tmpBag != null) {
                        switch (tmpBag.getLevel()) {
                            case 1:
                                res.addFascicoloSecondario(
                                        getClassificaParerByString(tmpBag.getClassificaFascicolo()), // classificazione
                                        String.valueOf(tmpBag.getAnnoFascicolo()), // anno data creazione fascicolo
                                        String.valueOf(tmpBag.getNumeroFasciolo()), // numero del fascicolo
                                        tmpBag.getNomeFascicolo(), // nome del fascicolo
                                        "", // numero sotto-fascicolo
                                        "", // nome sotto-fascicolo
                                        "", // numero inserto
                                        "");                                                         // nome inserto
                                titoliFascicoli.add(tmpBag.getClassificaFascicolo().replaceAll("/", "-"));
                                break;

                            case 2:
                                res.addFascicoloSecondario(
                                        getClassificaParerByString(tmpBag.getClassificaFascicolo()), // classificazione
                                        String.valueOf(tmpBag.getAnnoFascicolo()), // anno data creazione fascicolo
                                        String.valueOf(tmpBag.getNumeroFasciolo()), // numero del fascicolo
                                        tmpBag.getNomeFascicolo(), // nome del fascicolo
                                        String.valueOf(tmpBag.getNumeroSottoFascicolo()), // numero sotto-fascicolo
                                        tmpBag.getNomeSottoFascicolo(), // nome sotto-fascicolo
                                        "", // numero inserto
                                        "");                                                         // nome inserto
                                titoliFascicoli.add(tmpBag.getClassificaFascicolo().replaceAll("/", "-"));
                                break;

                            case 3:
                                res.addFascicoloSecondario(
                                        getClassificaParerByString(tmpBag.getClassificaFascicolo()), // classificazione
                                        String.valueOf(tmpBag.getAnnoFascicolo()), // anno data creazione fascicolo
                                        String.valueOf(tmpBag.getNumeroFasciolo()), // numero del fascicolo
                                        tmpBag.getNomeFascicolo(), // nome del fascicolo
                                        String.valueOf(tmpBag.getNumeroSottoFascicolo()), // numero sotto-fascicolo
                                        tmpBag.getNomeSottoFascicolo(), // nome sotto-fascicolo
                                        String.valueOf(tmpBag.getNumeroInserto()), // numero inserto
                                        tmpBag.getNomeInserto());                                       // nome inserto
                                titoliFascicoli.add(tmpBag.getClassificaFascicolo().replaceAll("/", "-"));
                                break;
                        }
                    }
                }
            }
        }

        // trattamento dei titoli extra fascicoli
        ArrayList<String> arrayTitoliDocumento = getArrayTitoliDocumento(gdDoc.getDatiParerGdDoc().getXmlSpecifico());

        if (arrayTitoliDocumento != null) {
            // lista contenente solo il titoli non direttamente collegati al fascicolo
            List<String> subtractList = UtilityFunctions.subtractList(arrayTitoliDocumento, titoliFascicoli);

            for (String r : subtractList) {
                String classifica = r.replaceAll("-", ".");
                res.addFascicoloSecondario(classifica, null, null, null, null, null, null, null);
            }
        }

        return res;
    }

    /**
     * Trasformo la classificazione nella rappresentazione accettata dal PaER
     *
     * @param c
     * @return classificazione separata da '.' anzich?? da '/'
     */
    private String getClassificaParerByString(String c) {

        String res = c.replaceAll("/", ".");

        return res;
    }

    /**
     * Ottiene Id a partire da Guid (INDE)
     *
     * @param list di guid di GdDoc
     * @return lista di idGdDoc
     * @throws SQLException
     */
    public static ArrayList<String> getIdFromGuid(ArrayList<String> list) throws SQLException {

        ArrayList<String> idList = new ArrayList<>();

        String guidList = "'" + StringUtils.join(list, "','") + "'";

        String sqlText
                = "SELECT  id_gddoc "
                + "FROM " + ApplicationParams.getGdDocsTableName() + " "
                + "WHERE guid_gddoc in (" + guidList + ") ";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(sqlText)) {
            log.info("eseguo la query: " + ps.toString() + "...");
            ResultSet res = ps.executeQuery();
            log.info("eseguita");
            while (res.next()) {
                idList.add(res.getString("id_gddoc"));
            }
        } catch (SQLException | NamingException ex) {
            log.fatal("errore nel reperire id_gdoc da guid_gddoc " + ex);
        }
        return idList;
    }

    /**
     * Ottiene la descrizione del riferimento temporale
     *
     * @param tipoDocumento
     * @return la descrizione scelta
     */
    private String getDescrizioneRiferimentoTemporale(String tipoDocumento) {
        String res = null;

        switch (tipoDocumento) {
            case "DOCUMENTO PROTOCOLLATO IN ENTRATA":
            case "DOCUMENTO PROTOCOLLATO IN USCITA":
            case "DOCUMENTO PROTOCOLLATO":
                res = "DATA_DI_PROTOCOLLAZIONE";
                break;

            case "DETERMINA":
            case "REGISTRO GIORNALIERO":
                res = "DATA_DI_REGISTRAZIONE";
                break;

            case "DELIBERAZIONE":
                res = "DATA DI INIZIO PUBBLICAZIONE";
                break;

            default:
                res = "";
        }
        return res;
    }

    /**
     * Parse da String a JSON
     *
     * @param str
     * @param key
     * @return JSONObject
     */
    private JSONObject getJson(String str, String key) {
        return (JSONObject) JSONValue.parse(str);
    }

    private String getStringFromJson(String str, String key) {
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(str);
        return (String) jsonXmlSpecifico.get(key);
    }

    /**
     * Traduzione dalla String ad un array di IdentityFile
     *
     * @param str
     * @param key
     * @return array di IdenityFile
     */
    private ArrayList<IdentityFile> getArrayIdentityFileFromJson(String str, String key) {
        ArrayList<IdentityFile> res = null;
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(str);
        String value = (String) jsonXmlSpecifico.get(key);

        if (value != null && !value.equals("")) {
            res = new ArrayList<>();

            JSONArray jsonArray = (JSONArray) JSONValue.parse(value);

            for (int i = 0; i < jsonArray.size(); i++) {
                String s = (String) jsonArray.get(i);
                JSONObject jtmp = (JSONObject) JSONValue.parse(s);
                IdentityFile identityFile = IdentityFile.parse(jtmp);
                res.add(identityFile);
            }
        }
        return res;
    }

    /**
     * Traduzione dalla String ad un array di InfoDocumento
     *
     * @param str
     * @param key
     * @return array di IdenityFile
     */
    private ArrayList<InfoDocumento> getArrayInfoDocumentoFromJson(String str, String key) {
        ArrayList<InfoDocumento> res = null;
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(str);
        String value = (String) jsonXmlSpecifico.get(key);

        if (value != null && !value.equals("")) {
            res = new ArrayList<>();

            JSONArray jsonArray = (JSONArray) JSONValue.parse(value);

            for (int i = 0; i < jsonArray.size(); i++) {
                String s = (String) jsonArray.get(i);
                JSONObject jtmp = (JSONObject) JSONValue.parse(s);
                InfoDocumento infoDoc = InfoDocumento.parse(jtmp);
                res.add(infoDoc);
            }
        }
        return res;
    }

    private IdentityFile getIdentityFileFromJson(String str, String key) {
        IdentityFile res = null;
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(str);
        String value = (String) jsonXmlSpecifico.get(key);
        if (value != null && !value.equals("")) {
            JSONObject jsonObj = (JSONObject) JSONValue.parse(value);
            res = IdentityFile.parse(jsonObj);
        }
        return res;
    }

    /**
     * In base alla movimentazione nei parametri specifici, setta la keyword
     * concordata con il ParER per indicare il tipo dell'unit?? documentaria
     *
     * @param gddoc
     * @param key
     * @return keyword concordata
     */
    private String getTipoDocumentoUnitaDocumentaria(GdDoc gddoc, String key) {

        String res = null;

        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(gddoc.getDatiParerGdDoc().getXmlSpecifico());

        // estrazione della stringa specifica dal json
        String str = (String) jsonXmlSpecifico.get(key);

        switch (str) {

            case "in":
                res = "DOCUMENTO PROTOCOLLATO IN ENTRATA";
                break;

            case "out":
                res = "DOCUMENTO PROTOCOLLATO IN USCITA";
                break;

            case "Pico":
                res = "DOCUMENTO PROTOCOLLATO";
                break;

            case "Dete":
                res = "DETERMINA";
                break;

            case "Deli":
                res = "DELIBERAZIONE";
                break;

            case "RegistroRepertorio":
                res = "REGISTRO GIORNALIERO";
                break;

            default:
                res = null;
        }

        return res;
    }

    /**
     * Restituisce una mappa contene id di INDE
     *
     * @return
     * @throws IOException
     * @throws MalformedURLException
     * @throws SendHttpMessageException
     */
    private Map<String, String> getIndeId() throws IOException, MalformedURLException, SendHttpMessageException {

        Map<String, String> result = new HashMap<>();

        // contruisce la mappa dei parametri per la servlet (va passato solo un parametro che ?? il numero di id da generare)
        Map<String, byte[]> params = new HashMap<>();

        // ottene un docid
        int idNumber = 1;
        params.put(GENERATE_INDE_NUMBER_PARAM_NAME, String.valueOf(idNumber).getBytes());

        String res = UtilityFunctions.sendHttpMessage(ApplicationParams.getGetIndeUrlServiceUri(), null, null, params, "POST", null);

        // la servlet torna un JsonObject che ?? una coppia (id, guid)
        JSONArray indeIdArray = (JSONArray) JSONValue.parse(res);
        JSONObject currentId = (JSONObject) indeIdArray.get(0);
        result.put(INDE_DOCUMENT_ID_PARAM_NAME, (String) currentId.get(INDE_DOCUMENT_ID_PARAM_NAME));
        result.put(INDE_DOCUMENT_GUID_PARAM_NAME, (String) currentId.get(INDE_DOCUMENT_GUID_PARAM_NAME));
        return result;
    }

    /**
     * Ottiene il formato delle date conforme allo standard del ParER
     *
     * @param data
     * @return la stringa con la data formattata opportunamente
     */
    private String getStringDateParer(DateTime data) {
        DecimalFormat mFormat = new DecimalFormat("00");
        int gg = data.getDayOfMonth();
        int mm = data.getMonthOfYear();
        int aa = data.getYear();
        int hour = data.getHourOfDay();
        int minute = data.getMinuteOfHour();
        int second = data.getSecondOfMinute();
        String res = mFormat.format(Double.valueOf(gg)) + "-" + mFormat.format(Double.valueOf(mm))
                + "-" + String.valueOf(aa) + " " + mFormat.format(Double.valueOf(hour)) + ":"
                + mFormat.format(Double.valueOf(minute)) + ":"
                + mFormat.format(Double.valueOf(second));
        return res;
    }

    private String getStringBoolean(boolean b) {
        return String.valueOf(b).toUpperCase();
    }

    /**
     * Funzione di estrazione stringa da JSONObject
     *
     * @param xmlSpecifico
     * @param key
     * @return
     */
    private String getStringFromJsonObject(String xmlSpecifico, String key) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(xmlSpecifico);
        return (String) jsonObject.get(key);
    }

    /**
     * Funzione di estrazione dei dati specifici da xml specifico
     *
     * @param xmlSpecifico
     * @param key
     * @return oggetto DatiSpecifici
     * @throws UnsupportedEncodingException
     */
    private DatiSpecifici getDatiSpecifici(String xmlSpecifico, String key) throws UnsupportedEncodingException {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(xmlSpecifico);
        String strDatiSpecifici = (String) jsonObject.get(key);
        // unmarshall in oggetto DatiSpecifici
        return DatiSpecifici.parser(strDatiSpecifici);
    }

    /**
     * Ottiene l'array dei titoli del documento fuori fascicolazione (per il
     * pregresso)
     *
     * @param xmlSpecifico
     * @return
     */
    private ArrayList<String> getArrayTitoliDocumento(String xmlSpecifico) {
        ArrayList<String> res = null;
        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(xmlSpecifico);
        String titoliDocumento = (String) jsonXmlSpecifico.get("titoliDocumento");
        if (titoliDocumento != null) {
            JSONArray jsonArrayTitoli = (JSONArray) JSONValue.parse(titoliDocumento);
            if (jsonArrayTitoli != null && !jsonArrayTitoli.equals("")) {
                res = new ArrayList<>();
                for (int i = 0; i < jsonArrayTitoli.size(); i++) {
                    res.add((String) jsonArrayTitoli.get(i));
                }
            }
        }
        return res;
    }

    /**
     * Ottiene l'oggetto GdDoc con le collection: -> CLASSIFICAZIONI esclusi i
     * fascicoli speciali -> PUBBLICAZIONI solo quelle effettivamente pubblicate
     * all'albo
     *
     * @param idGdDoc
     * @return il GdDoc caricato
     * @throws SQLException
     * @throws NamingException
     * @throws NotAuthorizedException
     * @throws IodaDocumentException
     */
    private GdDoc getGdDocById(String idGdDoc) throws SQLException, NamingException, NotAuthorizedException, IodaDocumentException {

        GdDoc gdDoc = null;

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();) {
            this.prefix = UtilityFunctions.checkAuthentication(dbConnection, getIdApplicazione(), getTokenApplicazione());

            HashMap<String, Object> additionalData = new HashMap<String, Object>();
            additionalData.put("FASCICOLAZIONI", "LOAD");
            additionalData.put("PUBBLICAZIONI", "LOAD");

            gdDoc = IodaDocumentUtilities.getGdDocById(dbConnection, idGdDoc, additionalData, prefix);
            gdDoc.setId(idGdDoc);
        }
        return gdDoc;
    }

    /**
     * Controlla se ci sono i dati mancanti che verranno inseriti al posto dei
     * segnaposti nei dati specifici. Se tutto ?? ok allora il gddoc ?? pronto per
     * essere versato
     *
     * @param gddoc
     * @return
     * @throws SQLException
     * @throws NamingException
     */
    private boolean isVersabile(GdDoc gddoc) throws SQLException, NamingException {

        log.info("funzione isVersabile: inizio");
        boolean res = false;

        // se il gddoc non ?? gi?? idoneo allora si ritorna la non versabilit??
        if (!gddoc.getDatiParerGdDoc().getIdoneoVersamento()) {
            log.info("funzione isVersabile: documento non idoneo");
            return false;
        }

        // calcolo della prima fascicolazione
        Fascicolazione primaFascicolazione = null;
        DateTime dataPrimaFascicolazione = null;
        try {
            primaFascicolazione = ordinaFascicolazioni(gddoc.getFascicolazioni()).get(0);
            dataPrimaFascicolazione = primaFascicolazione.getDataFascicolazione();
        } catch (Exception ex) {
            log.error(ex);
            dataPrimaFascicolazione = null;
        }

        // in base alla movimentazione del documento
        switch (getStringFromJsonObject(gddoc.getDatiParerGdDoc().getXmlSpecifico(), "movimentazione")) {

            case "in":
                if (getCanSendPicoEntrata()) {
                    if (dataPrimaFascicolazione == null) {
                        dataPrimaFascicolazione = gddoc.getDataRegistrazione();
                    }
                    replacePlaceholder(gddoc, dataPrimaFascicolazione, null, false);
                    res = true;
//                    if (dataPrimaFascicolazione != null){
//                        replacePlaceholder(gddoc, dataPrimaFascicolazione, null, false);
//                        res = true;
//                    }
                }
                break;

            case "out":
                if (getCanSendPicoUscita()) {
                    if (dataPrimaFascicolazione != null) {
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, null, false);
                        res = true;
                    }
                }
                break;

            case "Dete":
                if (getCanSendDete()) {
                    List<PubblicazioneIoda> pubblicazioni = gddoc.getPubblicazioni();
                    if (pubblicazioni != null && dataPrimaFascicolazione != null) {
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, pubblicazioni, false);
                        res = true;
                    }
//                    PubblicazioneIoda pubblicazione = getPrimaPubblicazione(gddoc.getPubblicazioni());
//                    if (pubblicazione != null && dataPrimaFascicolazione != null){
//                        replacePlaceholder(gddoc, dataPrimaFascicolazione, pubblicazione);
//                        res = true;
//                    }
                }
                break;

            case "Deli":
                if (getCanSendDeli()) {
                    List<PubblicazioneIoda> pubblicazioni = gddoc.getPubblicazioni();
                    if (pubblicazioni != null && dataPrimaFascicolazione != null) {
                        replacePlaceholder(gddoc, dataPrimaFascicolazione, pubblicazioni, false);
                        res = true;
                    }
//                    PubblicazioneIoda pubblicazione = getPrimaPubblicazione(gddoc.getPubblicazioni());
//                    if (pubblicazione != null && dataPrimaFascicolazione != null){
//                        replacePlaceholder(gddoc, dataPrimaFascicolazione, pubblicazione);
//                        res = true;
//                    }
                }
                break;

            case "RegistroRepertorio":
                if (getCanSendRegistroGiornaliero()) {

                    if (gddoc.getCodiceRegistro().equalsIgnoreCase("RGPICO") && getCanSendRgPico()) {
                        res = true;
                    } else if (gddoc.getCodiceRegistro().equalsIgnoreCase("RGDETE") && getCanSendRgDete()) {
                        res = true;
                    } else if (gddoc.getCodiceRegistro().equalsIgnoreCase("RGDELI") && getCanSendRgDeli()) {
                        res = true;
                    } else {
                        res = false;
                    }
                }
                break;

            default:
                res = false;
        }
        log.info("funzione isVersabile: fine con risultato " + res);
        return res;
    }

    /**
     * rimpiazza, nell'xml specifico, i segnaposti messi dalle applicazione con
     * i valori reali
     *
     * @param gddoc
     * @param dataPrimaFascicolazione
     * @param pubblicazione
     * @throws SQLException
     * @throws NamingException
     */
    private void replacePlaceholder(GdDoc gddoc, DateTime dataPrimaFascicolazione, List<PubblicazioneIoda> pubblicazioni, boolean prendiPubblicazioneEsecutiva) throws SQLException, NamingException {

        List<PubblicazioneIoda> pubblicazioniClone = null;
        if (pubblicazioni != null) {
            pubblicazioniClone = (List<PubblicazioneIoda>) ((ArrayList) pubblicazioni).clone();
        }

        // prendo xml specifico
        String xmlSpecifico = gddoc.getDatiParerGdDoc().getXmlSpecifico();

        // pattern prescelto per la rappresentazione della data
        String patternData = "yyyy-MM-dd";

        PubblicazioneIoda pubblicazione = getPrimaPubblicazione(pubblicazioniClone, prendiPubblicazioneEsecutiva);

        // gestione segnaposto prima pubblicazione (o pubblicazione con controllo regionale, quindi con esecutivit?? = inattesa)
        if (pubblicazione != null) {

            xmlSpecifico = xmlSpecifico.replace("[NUMEROPUBBLICAZIONE]", String.valueOf(pubblicazione.getNumeroPubblicazione()));
            xmlSpecifico = xmlSpecifico.replace("[ANNOPUBBLICAZIONE]", String.valueOf(pubblicazione.getAnnoPubblicazione()));
            xmlSpecifico = xmlSpecifico.replace("[INIZIOPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataDal(), patternData));
            xmlSpecifico = xmlSpecifico.replace("[FINEPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataAl(), patternData));

            if (pubblicazione.getEsecutivita().equalsIgnoreCase("esecutiva")) {
                xmlSpecifico = xmlSpecifico.replace("[CONTROLLOREGIONALE]", "NO");
                //xmlSpecifico = xmlSpecifico.replace("[DATAESECUTIVITA]", toIsoDateFormatString(pubblicazione.getDataDal(), patternData));
            } else {
                xmlSpecifico = xmlSpecifico.replace("[CONTROLLOREGIONALE]", "SI");
            }
            xmlSpecifico = xmlSpecifico.replace("[PUBBESECUTIVITA]", pubblicazione.getEsecutivita());

            // Seconda pubblicazione
            String dataPubblicazione = toIsoDateFormatString(pubblicazione.getDataDal(), patternData);
            pubblicazioniClone.remove(pubblicazione);

            if (pubblicazioniClone != null && pubblicazioniClone.size() > 0) {
                pubblicazione = getPrimaPubblicazione(pubblicazioniClone, prendiPubblicazioneEsecutiva);

                if (pubblicazione != null) {
                    xmlSpecifico = xmlSpecifico.replace("[NUMERORIPUBBLICAZIONE]", String.valueOf(pubblicazione.getNumeroPubblicazione()));
                    xmlSpecifico = xmlSpecifico.replace("[ANNORIPUBBLICAZIONE]", String.valueOf(pubblicazione.getAnnoPubblicazione()));
                    xmlSpecifico = xmlSpecifico.replace("[INIZIORIPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataDal(), patternData));
                    xmlSpecifico = xmlSpecifico.replace("[FINERIPUBBLICAZIONE]", toIsoDateFormatString(pubblicazione.getDataAl(), patternData));
                }
            }

            if (pubblicazione != null) {
                dataPubblicazione = toIsoDateFormatString(pubblicazione.getDataDal(), patternData);
            }

            xmlSpecifico = xmlSpecifico.replace("[DATAESECUTIVITA]", dataPubblicazione);
        }

        // gestione segnaposto fascicolazioni
        if (dataPrimaFascicolazione != null) {
            xmlSpecifico = xmlSpecifico.replace("[DATAFASCICOLAZIONE]", toIsoDateFormatString(dataPrimaFascicolazione, patternData));
        }

        // salvataggio su db
        DatiParerGdDoc dpg = gddoc.getDatiParerGdDoc();
        dpg.setXmlSpecifico(xmlSpecifico);
        updateDatiParerGdDoc(dpg, gddoc, null, null);
    }

    /**
     * esegue l'aggiornamento dei dati parer gddoc nella tabella
     * 'gd.dati_parer_gddoc'
     *
     * @param datiParer
     * @param gddoc
     * @throws SQLException
     * @throws NamingException
     */
    private void updateDatiParerGdDoc(DatiParerGdDoc datiParer, GdDoc gddoc, String xmlVersato, String errorMessage) throws SQLException, NamingException {

        VersatoreParerUtils util = new VersatoreParerUtils();

        String sqlText
                = "UPDATE " + ApplicationParams.getDatiParerGdDocTableName() + " SET "
                + "stato_versamento_proposto = coalesce(?, stato_versamento_proposto), "
                + "stato_versamento_effettivo = coalesce(?, stato_versamento_effettivo), "
                + "xml_specifico_parer = coalesce(?, xml_specifico_parer), "
                + "forza_conservazione = coalesce(?, forza_conservazione), "
                + "forza_accettazione = coalesce(?, forza_accettazione), "
                + "forza_collegamento = coalesce(?, forza_collegamento), "
                + "esito_ultimo_versamento = coalesce(?, esito_ultimo_versamento), "
                //+ "codice_errore_ultimo_versamento = coalesce(?, codice_errore_ultimo_versamento), "
                + "codice_errore_ultimo_versamento = ?, "
                + "documento_in_errore = coalesce(?, documento_in_errore) "
                + "WHERE id_gddoc = ? ";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, datiParer.getStatoVersamentoProposto());
            if (datiParer.getStatoVersamentoEffettivo() != null && !datiParer.getStatoVersamentoEffettivo().equals("")) {
                ps.setString(index++, datiParer.getStatoVersamentoEffettivo());
            } else {
                ps.setString(index++, null);
            }

            ps.setString(index++, datiParer.getXmlSpecifico());

            // forza_conservazione
            if (datiParer.getForzaConservazione() != null && datiParer.getForzaConservazione()) {
                ps.setInt(index++, -1);
            } else {
                ps.setInt(index++, 0);
            }

            // forza_accettazione
            if (datiParer.getForzaAccettazione() != null && datiParer.getForzaAccettazione()) {
                ps.setInt(index++, -1);
            } else {
                ps.setInt(index++, 0);
            }

            // forza_collegamento
            if (datiParer.getForzaCollegamento() != null && datiParer.getForzaCollegamento()) {
                ps.setInt(index++, -1);
            } else {
                ps.setInt(index++, 0);
            }

            // esito_ultimo_versamento
            if (datiParer.getEsitoUltimoVersamento() != null && !datiParer.getEsitoUltimoVersamento().equals("")) {
                ps.setString(index++, datiParer.getEsitoUltimoVersamento());
            } else {
                ps.setString(index++, null);
            }

            // codice_errore_ultimo_versamento
            if (datiParer.getCodiceErroreUltimoVersamento() != null && !datiParer.getCodiceErroreUltimoVersamento().equals("")) {
                ps.setString(index++, datiParer.getCodiceErroreUltimoVersamento());
            } else {
                ps.setString(index++, null);
            }

            // documento_in_errore
            String documentoInErrore = util.getNomeDocumentoInErrore(xmlVersato, errorMessage);

            if (documentoInErrore != null && !documentoInErrore.equals("")) {
                ps.setString(index++, documentoInErrore);
            } else {
                ps.setString(index++, null);
            }

            ps.setString(index++, gddoc.getId());

            String query = ps.toString();
            log.info("eseguo la query: " + query + " ...");
            int rowsUpdated = ps.executeUpdate();
            log.info("eseguita");
            if (rowsUpdated == 0) {
                throw new SQLException("Documento non trovato");
            } else if (rowsUpdated > 1) {
                log.fatal("troppe righe aggiornate; aggiornate " + rowsUpdated + " righe, dovrebbe essere una");
            }
        }
    }

    /**
     * ordina le fascicolazione in modo ASC
     *
     * @param fascicolazioni
     * @return fascicolazioni ordinate ASC
     */
    private List<Fascicolazione> ordinaFascicolazioni(List<Fascicolazione> fascicolazioni) {

        List<Fascicolazione> res = null;

        if (fascicolazioni != null) {
            if (fascicolazioni.size() > 0) {

                // ordino in ordine crescente
                try {
                    Collections.sort(fascicolazioni);
                    res = fascicolazioni;
                } catch (Exception ex) {
                    log.error("errore nella funzione di Collection.sort(): " + ex);
                    res = null;
                }
            } else {
                log.info("fascicolazioni = 0");
            }
        } else {
            log.info("fascicolazioni = null");
        }
        return res;
    }

    /**
     * se prendiPrimaEsecutiva = FALSE: estrae la prima pubblicazione (in
     * orodine cronologico) indipendentemente dall'esecutivit?? se
     * prendiPrimaEsecutiva = TRUE: estrae la prima pubblicazione (in orodine
     * cronologico) dove esecutivit?? = ESECUTIVA
     *
     * @param pubblicazioni
     * @return prima pubblicazione effettiva
     */
    private PubblicazioneIoda getPrimaPubblicazione(List<PubblicazioneIoda> pubblicazioni, boolean prendiPrimaEsecutiva) {

        PubblicazioneIoda res = null;

        if (pubblicazioni != null) {
            if (pubblicazioni.size() > 0) {

                // ordino in ordine crescente
                Collections.sort(pubblicazioni);

                for (PubblicazioneIoda pubblicazioneIoda : pubblicazioni) {

                    if (prendiPrimaEsecutiva) {
                        if (pubblicazioneIoda.getAnnoPubblicazione() != null && pubblicazioneIoda.getAnnoPubblicazione() != 0
                                && pubblicazioneIoda.getNumeroPubblicazione() != null && pubblicazioneIoda.getNumeroPubblicazione() != 0
                                && pubblicazioneIoda.getTipologia() == PubblicazioneIoda.Tipologia.ALBO
                                && pubblicazioneIoda.getEsecutivita().equalsIgnoreCase("esecutiva")) {
                            res = pubblicazioneIoda;
                            break;
                        }
                    } else {
                        if (pubblicazioneIoda.getAnnoPubblicazione() != null && pubblicazioneIoda.getAnnoPubblicazione() != 0
                                && pubblicazioneIoda.getNumeroPubblicazione() != null && pubblicazioneIoda.getNumeroPubblicazione() != 0
                                && pubblicazioneIoda.getTipologia() == PubblicazioneIoda.Tipologia.ALBO) {
                            res = pubblicazioneIoda;
                            break;
                        }
                    }
                }
            } else {
                log.info("pubblicazioni = 0");
            }
        } else {
            log.info("pubblicazioni = null");
        }
        return res;
    }

    /**
     * converte il DateTime con un pattern passato
     *
     * @param dateTime
     * @param pattern
     * @return data in form adi stringa
     */
    private String toIsoDateFormatString(DateTime dateTime, String pattern) {
        DateTimeFormatter dtfOut = DateTimeFormat.forPattern(pattern);
        return dtfOut.print(dateTime);
    }

    /**
     * metodi get/get dei parametri inseriti nel conf.json
     */
    public boolean getCanSendPicoUscita() {
        return canSendPicoUscita;
    }

    public void setCanSendPicoUscita(boolean canSendPicoUscita) {
        this.canSendPicoUscita = canSendPicoUscita;
    }

    public boolean getCanSendPicoEntrata() {
        return canSendPicoEntrata;
    }

    public void setCanSendPicoEntrata(boolean canSendPicoEntrata) {
        this.canSendPicoEntrata = canSendPicoEntrata;
    }

    public boolean getCanSendDete() {
        return canSendDete;
    }

    public void setCanSendDete(boolean canSendDete) {
        this.canSendDete = canSendDete;
    }

    public boolean getCanSendDeli() {
        return canSendDeli;
    }

    public void setCanSendDeli(boolean canSendDeli) {
        this.canSendDeli = canSendDeli;
    }

    public boolean getCanSendRegistroGiornaliero() {
        return canSendRegistroGiornaliero;
    }

    public void setCanSendRegistroGiornaliero(boolean canSendRegistroGiornaliero) {
        this.canSendRegistroGiornaliero = canSendRegistroGiornaliero;
    }

    public boolean getCanSendRegistroAnnuale() {
        return canSendRegistroAnnuale;
    }

    public void setCanSendRegistroAnnuale(boolean canSendRegistroAnnuale) {
        this.canSendRegistroAnnuale = canSendRegistroAnnuale;
    }

    public boolean getCanSendRgPico() {
        return canSendRgPico;
    }

    public void setCanSendRgPico(boolean canSendRgPico) {
        this.canSendRgPico = canSendRgPico;
    }

    public boolean getCanSendRgDete() {
        return canSendRgDete;
    }

    public void setCanSendRgDete(boolean canSendRgDete) {
        this.canSendRgDete = canSendRgDete;
    }

    public boolean getCanSendRgDeli() {
        return canSendRgDeli;
    }

    public void setCanSendRgDeli(boolean canSendRgDeli) {
        this.canSendRgDeli = canSendRgDeli;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getIdApplicazione() {
        return idApplicazione;
    }

    public void setIdApplicazione(String idApplicazione) {
        this.idApplicazione = idApplicazione;
    }

    public String getTokenApplicazione() {
        return tokenApplicazione;
    }

    public void setTokenApplicazione(String tokenApplicazione) {
        this.tokenApplicazione = tokenApplicazione;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getVersione() {
        return versione;
    }

    public void setVersione(String versione) {
        this.versione = versione;
    }

    public String getAmbiente() {
        return ambiente;
    }

    public void setAmbiente(String ambiente) {
        this.ambiente = ambiente;
    }

    public String getEnte() {
        return ente;
    }

    public void setEnte(String ente) {
        this.ente = ente;
    }

    public String getStrutturaVersante() {
        return strutturaVersante;
    }

    public void setStrutturaVersante(String strutturaVersante) {
        this.strutturaVersante = strutturaVersante;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getTipoComponenteDefault() {
        return tipoComponenteDefault;
    }

    public void setTipoComponenteDefault(String tipoComponenteDefault) {
        this.tipoComponenteDefault = tipoComponenteDefault;
    }

    public String getCodifica() {
        return codifica;
    }

    public void setCodifica(String codifica) {
        this.codifica = codifica;
    }

    public boolean getUseFakeId() {
        return useFakeId;
    }

    public void setUseFakeId(boolean useFakeId) {
        this.useFakeId = useFakeId;
    }

    public String getDateFrom() {
        return dateFrom;
    }

    public void setDateFrom(String dateFrom) {
        this.dateFrom = dateFrom;
    }

    public String getDateTo() {
        return dateTo;
    }

    public void setDateTo(String dateTo) {
        this.dateTo = dateTo;
    }

    /**
     * Controlla che il servizio non sia stato gi?? eseguito oggi. Per farlo
     * legge la data di fine dalla tabella bds_tools.servizi
     *
     * @param dbConn
     * @return "true" se il servizio non ?? stato eseguito oggi, "false"
     * altrimenti
     * @throws SQLException
     */
    private boolean nonFattoOggi(Connection dbConn) throws SQLException {
        String query = ""
                + "select data_fine, data_inizio "
                + "from bds_tools.servizi "
                + "where nome_servizio = ? and id_applicazione = ?";
        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setString(1, getClass().getSimpleName());
            ps.setString(2, ID_APPLICAZIONE);
            log.info(String.format("eseguo la query: %s", ps.toString()));
            ResultSet res = ps.executeQuery();
            if (res.next()) {
                Date dataFine = null;
                Date dataInizio = null;
                try {
                    dataFine = res.getDate(1);
                    dataInizio = res.getDate(2);
                    if (dataFine != null && dataInizio != null) {
                        log.info(String.format("data_inizio letta: %s", dataInizio));
                        log.info(String.format("data_fine letta: %s", dataFine));

                        //in milliseconds
                        long diffFine = System.currentTimeMillis() - dataFine.getTime();
                        long diffDaysFine = diffFine / (24 * 60 * 60 * 1000);

                        long diffInizio = dataFine.getTime() - dataInizio.getTime();
                        long diffDaysInizio = diffInizio / (24 * 60 * 60 * 1000);

                        if ((diffDaysFine >= 1) || (diffDaysFine < 1 && diffDaysInizio >= 1)) {
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
//                    if (dataFine != null) {
//                        log.debug(String.format("data letta: %s", dataFine));
//
//                        //in milliseconds
//                        long diff = System.currentTimeMillis() - dataFine.getTime();
//                        long diffDays = diff / (24 * 60 * 60 * 1000);
//
//                        return diffDays > 0;
//                    } else {
//                        return false;
//                    }
                } catch (Exception ex) {
                    log.info("nonFattoOggi - errore nel reperimento della data_fine: " + ex);
                    return false;
                }

            } else {
                throw new SQLException(String.format("servizio %s dell'applicazione %s non trovato", getClass().getSimpleName(), ID_APPLICAZIONE));
            }
        }
    }

    /**
     * Controlla che l'ora sia maggiore di quella indicata dal parametro cron
     * nello schedulatore conf json.
     *
     * @return "true" se l'ora ?? maggiore di quella su cron (a ora uguale si
     * guardano i minuti), "false" altrimenti
     */
//    private boolean oraLancioIdonea() throws ParseException {
//
//        boolean result = false;
//
//        java.util.Date dataInConf;
//        CronExpression cronExpression;
//
//        cronExpression = new CronExpression(getRole());
//        dataInConf = cronExpression.getNextValidTimeAfter(new java.util.Date());
//        LocalDateTime now = LocalDateTime.now();
//
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(dataInConf);
//        int oraDB = cal.get(Calendar.HOUR_OF_DAY);
//        int minutiDB = cal.get(Calendar.MINUTE);
//
//        log.debug("ora letta da db: " + oraDB);
//        log.debug("minuti letti db: " + minutiDB);
//
//        log.debug("ora attuale: " + now.getHour());
//        log.debug("minuti attuali: " + now.getMinute());
//
//        if (now.getHour() == oraDB){
//            if (now.getMinute() > minutiDB){
//                System.out.println("pu?? partire");
//                result = true;
//            }
//            else{
//                System.out.println("non pu?? partire");
//                result = false;
//            }
//        }
//        else if(now.getHour() > oraDB){
//            System.out.println("pu?? partire");
//            result = true;
//        }
//        else{
//            System.out.println("non pu?? partire");
//            result = false;
//        }
//        return result;
//    }
    /**
     * Controlla se i servizi attivi del calcolo idoneit?? pro parer abbiano
     * finito
     *
     * @param dbConn
     * @return true se tutti i servizi attivi sono terminati; false altrimenti
     * @throws SQLException
     */
    private boolean serviziApplicativiFiniti(Connection dbConn) throws SQLException {

        boolean serviziFiniti = true;
        boolean serviziPicoDeteDeliAttivi = false;

        String query = ""
                + "select id_applicazione, data_fine "
                + "from bds_tools.servizi "
                + "where nome_servizio = ? and attivo != 0";

        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            ps.setString(1, SERVIZIO_IDONEITA);
            log.info(String.format("eseguo la query: %s", ps.toString()));
            ResultSet res = ps.executeQuery();

            while (res.next()) {
                serviziPicoDeteDeliAttivi = true;
                String id_applicazione = res.getString(1);

                try {
                    Date date = res.getDate(2);
                    if (date == null) {
                        log.info("servizio idoneita_parer_pico non finito");
                        serviziFiniti = serviziFiniti && false;
                    }
                } catch (Exception ex) {
                    log.info("servizio idoneita_parer su applicazione " + id_applicazione + "non ancora finito");
                    serviziFiniti = serviziFiniti && false;
                }
            }
            if (!serviziPicoDeteDeliAttivi) {
                serviziFiniti = true;
            }
            return serviziFiniti;
        }
    }

    /**
     * aggiorna le date di avvio e terminazione del servizio; quando un servizio
     * inizia a girare sar?? settata la data di inizio a now() e la data fine a
     * null qundo un servizio termina mette la data di fine a now()
     *
     * @param dataInizio
     * @param dataFine
     * @throws SQLException
     */
    private void updateDataInizioDataFine(Connection dbConn, Timestamp dataInizio, Timestamp dataFine) throws SQLException {
        String query = ""
                + "UPDATE bds_tools.servizi "
                + "SET "
                + "data_inizio = coalesce(?, data_inizio), "
                + "data_fine = ? "
                + "where nome_servizio = ? and id_applicazione = ?";

        try (PreparedStatement ps = dbConn.prepareStatement(query)) {
            int index = 1;
            ps.setTimestamp(index++, dataInizio);
            ps.setTimestamp(index++, dataFine);
            ps.setString(index++, getClass().getSimpleName());
            ps.setString(index++, ID_APPLICAZIONE);

            log.info(String.format("eseguo la query: %s...", ps.toString()));
            int res = ps.executeUpdate();
            if (res == 0) {
                throw new SQLException("errore nell'aggiornamento delle date inizio/fine, l'update ha tornato 0");
            }
        }
    }

    /**
     * Popola la tabella diagnostica.report. Prende gli errori tecnici
     * dell'ultimo versamento e li inserisce in questa tabella che sar??
     * utilizzata dal checkMK usando la tipologia PARER_ERR_TECNICO
     *
     * @param dataInserimento in formato stringa
     */
    private void riversaErroriInTabellaCheck(String dataInserimento) {

        String qNew
                = "INSERT INTO diagnostica.report(tipologia, data_inserimento_riga, additional_data) "
                + "select 'PARER_ERR_TECNICO' as tipologia, now() as data_inserimento_riga, json_build_object('guid_gddoc', g.guid_gddoc,'guid_ses_vers', vp.guid_sessione_versamento_parer, 'cod_err', sv.codice_errore, 'desc_err', sv.descrizione_errore, 'data_fin_vers', ?, 'cod_reg', g.codice_registro) "
                + "from gd.gddocs g, gd.gddocs_sessioni_versamento sv, gd.sessioni_versamento_parer vp "
                + "where sv.id_sessione_versamento_parer = vp.id_sessione_versamento_parer "
                + "and sv.id_gddoc = g.id_gddoc "
                + "and sv.id_sessione_versamento_parer = ( "
                + "select id_sessione_versamento_parer "
                + "from gd.sessioni_versamento_parer "
                + "where data_fine is not null "
                + "order by data_inizio DESC "
                + "limit 1 "
                + ") "
                + "and g.codice_registro != 'PGI' "
                + "and ((sv.esito = 'ERRORE' and g.forzato_da_utente != 0 and g.forzato_da_utente is not null) "
                + "or (sv.esito = 'ERRORE' and g.forzato_da_utente = 0 and sv.codice_errore != 'UD-008-001' and "
                + "(sv.codice_errore = 'FIRMA-005-001' or left(sv.codice_errore, 5) != 'FIRMA')))";

        try (Connection dbConnection = UtilityFunctions.getDBConnection(); PreparedStatement ps = dbConnection.prepareStatement(qNew)) {
            dbConnection.setAutoCommit(false);
            log.info("riversamento errori in tabella check");

            int index = 1;
            ps.setString(index++, dataInserimento);
            //ps.setTimestamp(index++, new Timestamp(dataInserimento.getMillis()));

            int rowsUpdated = ps.executeUpdate();
            log.info("eseguita");
            if (rowsUpdated == 0) {
                dbConnection.rollback();
                throw new SQLException("record di gddoc_sessione_versamento non inserito");
            }
            dbConnection.commit();
        } catch (SQLException | NamingException ex) {
            log.fatal("errore in riversaErroriInTabellaCheck: " + ex);
        }
    }

    public void inviaNotificaSuScrivania(ServiceRequestInformation serviceRequestInformation, String idSessioneVersamento, String dataFineVersamento) throws MasterChefClientException {

        UpdateBabelParams updateBabelParams = new UpdateBabelParams("babel", "babel", null, "false", "false", "insert");
        String idAttivita = UUID.randomUUID().toString();

        List<String> content = (List<String>) serviceRequestInformation.getContent();
        int documentiTotaliSelezionatiDaUtente = content.size();

        int esitoOK = 0;
        int esitoERRORE = 0;

        String sqlTextOK
                = "SELECT count(id_gddoc) "
                + "FROM gd.gddocs_sessioni_versamento "
                + "WHERE id_sessione_versamento_parer = ? "
                + "and esito = 'OK'";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(sqlTextOK)) {
            ps.setString(1, idSessioneVersamento);
            log.info("eseguo la query: " + ps.toString() + "...");
            ResultSet res = ps.executeQuery();
            log.info("eseguita");
            while (res.next()) {
                esitoOK = res.getInt("count");
            }
        } catch (SQLException | NamingException ex) {
            log.fatal("errore nel reperire id_gdoc da guid_gddoc " + ex);
        }

        String sqlTextERRORE
                = "SELECT count(id_gddoc) "
                + "FROM gd.gddocs_sessioni_versamento "
                + "WHERE id_sessione_versamento_parer = ? "
                + "and esito = 'ERRORE'";

        try (
                Connection dbConnection = UtilityFunctions.getDBConnection();
                PreparedStatement ps = dbConnection.prepareStatement(sqlTextERRORE)) {
            ps.setString(1, idSessioneVersamento);
            log.info("eseguo la query: " + ps.toString() + "...");
            ResultSet res = ps.executeQuery();
            log.info("eseguita");
            while (res.next()) {
                esitoERRORE = res.getInt("count");
            }
        } catch (SQLException | NamingException ex) {
            log.fatal("errore nel reperire id_gdoc da guid_gddoc " + ex);
        }

        updateBabelParams.addAttivita(
                idAttivita,
                null,
                serviceRequestInformation.getIdUtente(),
                "3",
                "esito versamento",
                calcolaMessaggioNotificaParer(documentiTotaliSelezionatiDaUtente, esitoOK, esitoERRORE, dataFineVersamento),
                null,
                "Versatore ParER",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "group_" + idAttivita,
                null,
                null);

        String retQueue = "notifica_versamento" + "_" + ApplicationParams.getAppId() + "_updateBabelRetQueue_" + ApplicationParams.getServerId();
        log.info("Creazione Worker..");
        WorkerData wd = new WorkerData(ApplicationParams.getAppId(), "1", retQueue);
        log.info("Worker creato");
        wd.addNewJob("1", null, updateBabelParams);

//          RedisClient rd = new RedisClient("gdml", null);
//          rd.put(wd.getStringForRedis(), "chefingdml");
        RedisClient rd = new RedisClient(ApplicationParams.getRedisHost(), null);
        log.info("Add Worker to redis..");
        rd.put(wd.getStringForRedis(), ApplicationParams.getRedisInQueue());
        log.info("Worker added to redis");

        String extractedValue = rd.bpop(retQueue, 86400);
        WorkerResponse wr = new WorkerResponse(extractedValue);
        if (wr.getStatus().equalsIgnoreCase("ok")) {
            log.info("invio notifica versamento effettuata con successo");
        } else {
            log.info("errore tornato da masterchef: " + wr.getError());
        }
    }

    private String calcolaMessaggioNotificaParer(int documentiTot, int documentiOK, int documentiERRORE, String dataStr) {

        String dataOra = changePatternDateString(DATE_PATTERN_STANDARD, DATE_PATTERN_FOR_USER, dataStr);

        String res = "Esito versamento forzato del "
                + dataOra
                + ": "
                + documentiERRORE
                + " documenti in errore, "
                + documentiOK + " documenti versati correttamente";

//        String res = "E' terminata l'esecuzione del versamento forzato richiesto il "
//                + "[" + dataOra + "], con esito: "
//                + documentiERRORE + " documenti in errore, "
//                + documentiOK + " documenti versati correttamente";
//        String res = documentiTot
//                + " documenti presi in carico. Esito di versamento: "
//                + documentiOK
//                + " documenti versati correttamente e "
//                + documentiERRORE
//                + " documenti in errore";
        return res;
    }

    private String changePatternDateString(String patternIn, String patternOut, String dataOraStr) {
        // formato di input
        DateTimeFormatter dtf = DateTimeFormat.forPattern(patternIn);
        // Parsing della data
        DateTime jodatime = dtf.parseDateTime(dataOraStr);
        // formato di putput
        DateTimeFormatter dtfOut = DateTimeFormat.forPattern(patternOut);
        // ritorna la data in stringa
        return dtfOut.print(jodatime);
    }

    // trasformo la classificazione nella rappresentazione accettata dal PaER
//    private String getClassificaParerByClassificazione(ClassificazioneFascicolo c){
//
//        String res = null;
//
//        if (c.getCodiceSottoclasse() != null && !c.getCodiceSottoclasse().equals("")){
//            res = c.getCodiceSottoclasse().replaceAll("/", ".");
//        }
//        else if (c.getCodiceClasse() != null && !c.getCodiceClasse().equals("")){
//            res = c.getCodiceClasse().replaceAll("/", ".");
//        }
//        else if (c.getCodiceCategoria()!= null && !c.getCodiceCategoria().equals("")){
//            res = c.getCodiceCategoria().replaceAll("/", ".");
//        }
//
//        return res;
//    }
    //    private Map<String, String> getCodiceNomeTitolo(ClassificazioneFascicolo c){
//        Map<String, String> res = null;
//
//        String codiceTitolo = null, nomeTitolo = null;
//
//        if (c.getCodiceSottoclasse() != null && !c.getCodiceSottoclasse().equals("")){
//            codiceTitolo = c.getCodiceSottoclasse();
//            nomeTitolo = c.getNomeSottoclasse();
//        }
//        else if (c.getCodiceClasse()!= null && !c.getCodiceClasse().equals("")){
//            codiceTitolo = c.getCodiceClasse();
//            nomeTitolo = c.getNomeClasse();
//        }
//        else if (c.getCodiceCategoria()!= null && !c.getCodiceCategoria().equals("")){
//            codiceTitolo = c.getCodiceCategoria();
//            nomeTitolo = c.getNomeCategoria();
//        }
//
//        res.put("codice", codiceTitolo);
//        res.put("nome", nomeTitolo);
//
//        return res;
//    }
//    private DatiSpecifici getDatiSpecifici(GdDoc gddoc) throws UnsupportedEncodingException{
//
//        DatiSpecifici res = null;
//
//        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(gddoc.getDatiParerGdDoc().getXmlSpecifico());
//
//        // estrazione della stringa specifica dal json
//        String strDatiSpecifici = (String) jsonXmlSpecifico.get("xmlpart");
//
//        //unmarshal nell'oggetto DatiSpecifici
//        res = DatiSpecifici.parser(strDatiSpecifici);
//
//        return res;
//    }
    //    private String getMovimentazione(String xmlSpecifico){
//        JSONObject jsonXmlSpecifico = (JSONObject) JSONValue.parse(xmlSpecifico);
//        String movimentazione = (String) jsonXmlSpecifico.get("movimentazione");
//        return movimentazione;
//    }
}
