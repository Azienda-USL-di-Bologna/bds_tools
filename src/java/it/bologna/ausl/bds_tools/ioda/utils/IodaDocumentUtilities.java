package it.bologna.ausl.bds_tools.ioda.utils;

import com.mongodb.MongoException;
import it.bologna.ausl.balboclient.classes.Pubblicazione;
import it.bologna.ausl.balboclient.classes.PubblicazioneAlbo;
import it.bologna.ausl.bds_tools.SetDocumentNumber;
import it.bologna.ausl.bds_tools.exceptions.NotAuthorizedException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.SupportedFile;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.IodaRequestDescriptor;
import it.bologna.ausl.ioda.iodaobjectlibrary.PubblicazioneIoda;
import it.bologna.ausl.ioda.iodaobjectlibrary.SimpleDocument;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento.TipoFirma;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaFileException;
import it.bologna.ausl.mimetypeutility.Detector;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.exceptions.MongoWrapperException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author gdm
 */
public class IodaDocumentUtilities {
private static final Logger log = LogManager.getLogger(IodaDocumentUtilities.class);
    
public static final String INDE_DOCUMENT_ID_PARAM_NAME = "document_id";
public static final String INDE_DOCUMENT_GUID_PARAM_NAME = "document_guid";

private static final String GENERATE_INDE_NUMBER_PARAM_NAME = "generateidnumber";

HttpServletRequest request;
private String prefixIds; // prefisso da anteporre agli id dei documenti che si inseriscono o che si ricercano (GdDoc, SottoDocumenti)
private MongoWrapper mongo;
private String mongoParentPath;
private String getIndeIdServletUrl;
private String gdDocTable;
private String sottoDocumentiTable;
private String fascicoliTable;
private String fascicoliGdDocTable;
private GdDoc gdDoc;
private JSONArray indeId;
private int indeIdIndex;
private Detector detector;

private final List<SottoDocumento> toConvert = new ArrayList<>();
private final List<String> uploadedUuids = new ArrayList<>();
private final List<String> uuidsToDelete = new ArrayList<>();

    private IodaDocumentUtilities(Document.DocumentOperationType operation, String prefixIds) throws UnknownHostException, MongoException, MongoWrapperException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.gdDocTable = ApplicationParams.getGdDocsTableName();
        String mongoUri = ApplicationParams.getMongoRepositoryUri();
        this.mongo = new MongoWrapper(mongoUri);
        this.sottoDocumentiTable = ApplicationParams.getSottoDocumentiTableName();
        this.prefixIds = prefixIds;
        
        if (operation != Document.DocumentOperationType.DELETE) {
            this.detector = new Detector();
            this.indeIdIndex = 0; 
            this.mongoParentPath = ApplicationParams.getUploadGdDocMongoPath();
            this.getIndeIdServletUrl = ApplicationParams.getGetIndeUrlServiceUri();
            this.fascicoliTable = ApplicationParams.getFascicoliTableName();
            this.fascicoliGdDocTable = ApplicationParams.getFascicoliGdDocsTableName();
        }
    }

    public IodaDocumentUtilities(IodaRequestDescriptor iodaRequestDescriptor, Document.DocumentOperationType operation, String prefixIds) throws UnknownHostException, MongoException, MongoWrapperException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(operation, prefixIds);
        this.gdDoc = iodaRequestDescriptor.getGdDoc(operation);
        this.gdDoc.setPrefissoApplicazioneOrigine(this.prefixIds);

        if (operation != Document.DocumentOperationType.DELETE) {
            getIndeId();
            if (operation == Document.DocumentOperationType.INSERT) {
                JSONObject nextIndeId = getNextIndeId();
                this.gdDoc.setId((String) nextIndeId.get(INDE_DOCUMENT_ID_PARAM_NAME));
                this.gdDoc.setGuid((String) nextIndeId.get(INDE_DOCUMENT_GUID_PARAM_NAME));
            }
        }
    }

    public GdDoc getGdDoc() {
        return gdDoc;
    }

    private JSONObject getNextIndeId() {
        JSONObject currentId = (JSONObject) indeId.get(indeIdIndex++);
        return currentId;
    }

    public String getMongoParentPath() {
        return mongoParentPath;
    }
    
    public String getMongoPath() {
        // mongopath: tipoOggettoOrigine_idOggettoOrigine
        return mongoParentPath + "/" + gdDoc.getIdOggettoOrigine() + "_" + gdDoc.getTipoOggettoOrigine();
    }

    public void insertFascicolazione(Connection dbConn, Fascicolazione fascicolazione) throws SQLException {
//        String idFascicolo = getIdFascicolo(dbConn, ps, fascicolo);
        
        JSONObject newIds = getNextIndeId();
        String idFascicoloGdDoc = (String) newIds.get(INDE_DOCUMENT_ID_PARAM_NAME);
//        String guidFascicoloGdDoc = (String) newIds.get(INDE_DOCUMENT_GUID_PARAM_NAME);
        
        String sqlText = 
                "INSERT INTO " + getFascicoliGdDocTable() + "(" +
                "id_fascicolo_gddoc, id_gddoc, id_fascicolo, data_assegnazione, id_utente_fascicolatore) " +
                "VALUES (?, ?, (select id_fascicolo from gd.fascicoligd where numerazione_gerarchica = ?), ?, ?)";
        
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, idFascicoloGdDoc);
            ps.setString(index++, gdDoc.getId());
            ps.setString(index++, fascicolazione.getCodiceFascicolo());
            ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
            ps.setString(index++, fascicolazione.getIdUtenteFascicolatore());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("Fascicolo non inserito");
        }
    }

    public void insertPubblicazione(Connection dbConn, PubblicazioneIoda pubblicazione) throws SQLException {
        String sqlText = 
                "INSERT INTO " + ApplicationParams.getPubblicazioniAlboTableName() + "(" +
                "numero_pubblicazione, anno_pubblicazione, data_dal, data_al, id_gddoc, pubblicatore, esecutivita, data_defissione, data_esecutivita, esecutiva) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            if (pubblicazione.getNumeroPubblicazione() != null)
                ps.setLong(index++, pubblicazione.getNumeroPubblicazione());
            else
                ps.setNull(index++, Types.BIGINT);
            if (pubblicazione.getAnnoPubblicazione() != null)
                ps.setInt(index++, pubblicazione.getAnnoPubblicazione());
            else
                ps.setNull(index++, Types.INTEGER);
            if (pubblicazione.getDataDal() != null)
                ps.setTimestamp(index++, new Timestamp(pubblicazione.getDataDal().getMillis()));
            else
                ps.setNull(index++, Types.TIMESTAMP);
            if (pubblicazione.getDataAl() != null)
                ps.setTimestamp(index++, new Timestamp(pubblicazione.getDataAl().getMillis()));
            else
                ps.setNull(index++, Types.TIMESTAMP);
            ps.setString(index++, gdDoc.getId());
            
            ps.setString(index++, pubblicazione.getPubblicatore());
            
            ps.setString(index++, pubblicazione.getEsecutivita());
            
            if (pubblicazione.getDataDefissione() != null)
                ps.setTimestamp(index++, new Timestamp(pubblicazione.getDataDefissione().getMillis()));
            else
                ps.setNull(index++, Types.TIMESTAMP);

            if (pubblicazione.getDataEsecutivita() != null)
                ps.setTimestamp(index++, new Timestamp(pubblicazione.getDataEsecutivita().getMillis()));
            else
                ps.setNull(index++, Types.TIMESTAMP);

            ps.setInt(index++, pubblicazione.isEsecutiva() ? -1: 0);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("Fascicolo non inserito");
        }
    }
    
    public void deleteFascicolazioni(Connection dbConn) throws SQLException {
        String sqlText = 
                "DELETE FROM " + getFascicoliGdDocTable() + " " + 
                "WHERE id_gddoc = ?";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, gdDoc.getId());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
        }
    }
    
    public void deleteFascicolazione(Connection dbConn, Fascicolazione fascicolazione) throws SQLException {
    String sqlText = 
                "DELETE FROM " + getFascicoliGdDocTable() + " " + 
                "WHERE id_fascicolo_gddoc in (" +
                    "SELECT fg.id_fascicolo_gddoc " +
                    "FROM " + getFascicoliGdDocTable() + " fg INNER JOIN " +
                            getFascicoliTable() + " f ON fg.id_fascicolo = f.id_fascicolo INNER JOIN " +
                            getGdDocTable() + " g ON fg.id_gddoc = g.id_gddoc " +
                    "WHERE f.numerazione_gerarchica = ? AND g.id_oggetto_origine = ? AND g.tipo_oggetto_origine = ? " +
                ")";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, fascicolazione.getCodiceFascicolo());
            ps.setString(index++, gdDoc.getIdOggettoOrigine());
            ps.setString(index++, gdDoc.getTipoOggettoOrigine());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("fascicolazione con codice: " + fascicolazione.getCodiceFascicolo() + " non trovata");
        }
    }

    public void updateFascicolazione(Connection dbConn, Fascicolazione fascicolazione) throws SQLException {
    String sqlText = 
                "UPDATE " + getFascicoliGdDocTable() + " SET " + 
                "id_utente_fascicolatore = coalesce(?, id_utente_fascicolatore), " +
                "data_eliminazione = coalesce(?, data_eliminazione), " +
                "id_utente_eliminazione = coalesce(?, id_utente_eliminazione) " +
                "WHERE id_fascicolo_gddoc in (" +
                    "SELECT fg.id_fascicolo_gddoc " +
                    "FROM " + getFascicoliGdDocTable() + " fg INNER JOIN " +
                              getFascicoliTable() + " f ON fg.id_fascicolo = f.id_fascicolo INNER JOIN " +
                              getGdDocTable() + " g ON fg.id_gddoc = g.id_gddoc " +
                    "WHERE f.numerazione_gerarchica = ? AND g.id_oggetto_origine = ? AND g.tipo_oggetto_origine = ? " +
                ")";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, fascicolazione.getIdUtenteFascicolatore());
            if (fascicolazione.getDataEliminazione() != null) {
                Timestamp dataEliminazione = new Timestamp(fascicolazione.getDataEliminazione().getMillis());
                ps.setTimestamp(index++, dataEliminazione);
            }
            else
                ps.setNull(index++, Types.TIMESTAMP);
            ps.setString(index++, fascicolazione.getIdUtenteEliminatore());

            ps.setString(index++, fascicolazione.getCodiceFascicolo());
            ps.setString(index++, gdDoc.getIdOggettoOrigine());
            ps.setString(index++, gdDoc.getTipoOggettoOrigine());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("fascicolazione con codice: " + fascicolazione.getCodiceFascicolo() + " non trovata");
        }
    }

    @Deprecated
    private String getIdFascicolo(Connection dbConn, PreparedStatement ps, Fascicolo f) throws SQLException {
        String sqlText =
                    "SELECT id_fascicolo " +
                    "FROM " + getFascicoliTable() + " " +
                    "WHERE numerazione_gerarchica = ?";
        ps = dbConn.prepareStatement(sqlText);
        int index = 1;
        ps.setString(index++, f.getCodiceFascicolo());
        
        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        ResultSet results = ps.executeQuery();
        if (!results.next())
            throw new SQLException("nessun fascicolo trovato");
        return results.getString(1);
        
    }

    /**
     * torna un oggetto GdDoc rapprestato dall'idOggettoOrigine e tipoOggettoOrigine presente nel SimpleDocument all'interno della IodaRequestDescriptor passata
     * @param dbConn connessione al db da usare
     * @param doc un oggetto SimpleDocument che rappresenta il GdDoc da caricare
     * @param collections una mappa Map<String, Boolean> con indicate le collection da caricare
     * @param prefix il prefisso dell'applicazione
     * @return un oggetto GdDoc rapprestato dall'idOggettoOrigine e tipoOggettoOrigine presente nel SimpleDocument all'interno della IodaRequestDescriptor passata
     * @throws java.sql.SQLException
     * @throws it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException
     */
    public static GdDoc getGdDoc(Connection dbConn, SimpleDocument doc, Map<String, Object> collections, String prefix) throws SQLException, IodaDocumentException {
        doc.setPrefissoApplicazioneOrigine(prefix);

        String sqlText = 
                "SELECT  id_gddoc, " +
                        "applicazione, " +
                        "codice, " +
                        "codice_registro, " +
                        "data_gddoc, " +
                        "data_registrazione, " +
                        "data_ultima_modifica, " +
                        "id_oggetto_origine, " +
                        "nome_gddoc, " +
                        "nome_struttura_firmatario, " +
                        "numero_registrazione, " +
                        "oggetto, " +
                        "tipo_gddoc, " +
                        "tipo_oggetto_origine, " +
                        "stato_gd_doc, " +
                        "url_command, " +
                        "id_utente_creazione " +
                        "FROM " + ApplicationParams.getGdDocsTableName() + " " +
                        "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";

        GdDoc gdDoc = null;
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, doc.getIdOggettoOrigine());
            ps.setString(index++, doc.getTipoOggettoOrigine());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();

            String idGdDoc;  
            if (result != null && result.next()) {
                idGdDoc = result.getString("id_gddoc");
                gdDoc = new GdDoc();
                gdDoc.setApplicazione(result.getString("applicazione"));
                gdDoc.setCodice(result.getString("codice"));
                gdDoc.setCodiceRegistro(result.getString("codice_registro"));
                
                Timestamp dataGdDoc = result.getTimestamp("data_gddoc");
                if (dataGdDoc != null)
                    gdDoc.setData(new DateTime(dataGdDoc.getTime()));
                
                Timestamp dataRegistrazione = result.getTimestamp("data_registrazione");
                if (dataRegistrazione != null)
                    gdDoc.setDataRegistrazione(new DateTime(dataRegistrazione.getTime()));
                
                Timestamp dataUltimaModifica = result.getTimestamp("data_ultima_modifica");
                if (dataUltimaModifica != null)
                    gdDoc.setDataUltimaModifica(new DateTime(dataUltimaModifica.getTime()));
                

                gdDoc.setIdOggettoOrigine(result.getString("id_oggetto_origine"));
                gdDoc.setNome(result.getString("nome_gddoc"));
                gdDoc.setNomeStrutturaFirmatario(result.getString("nome_struttura_firmatario"));
                gdDoc.setNumeroRegistrazione(result.getString("numero_registrazione"));
                gdDoc.setOggetto(result.getString("oggetto"));
                gdDoc.setRecord(result.getString("tipo_gddoc") == null || result.getString("tipo_gddoc").equalsIgnoreCase("r"));
                gdDoc.setTipoOggettoOrigine(result.getString("tipo_oggetto_origine"));
                gdDoc.setVisibile(result.getInt("stato_gd_doc") != 0);
                gdDoc.setUrlCommand(result.getString("url_command"));
                gdDoc.setIdUtenteCrezione(result.getString("id_utente_creazione"));
                
                if (result.next())
                    throw new IodaDocumentException("trovato più di un GdDoc, questo non dovrebbe accadere");

                Object collection = collections.get(GdDoc.GdDocCollectionNames.FASCICOLAZIONI.toString());
                if (collection != null && ((String)collection).equalsIgnoreCase(GdDoc.GdDocCollectionValues.LOAD.toString())) {
                    log.debug("carico: " + GdDoc.GdDocCollectionNames.FASCICOLAZIONI.toString() + "...");
                    gdDoc.setFascicolazioni(caricaFascicolazioni(dbConn, doc, prefix));
                }

                collection = collections.get(GdDoc.GdDocCollectionNames.PUBBLICAZIONI.toString());
                if (collection != null && ((String)collection).equalsIgnoreCase(GdDoc.GdDocCollectionValues.LOAD.toString())) {
                    log.debug("carico: " + GdDoc.GdDocCollectionNames.PUBBLICAZIONI.toString() + "...");
                    gdDoc.setPubblicazioni(caricaPubblicazioni(dbConn, idGdDoc));
                }

                collection = collections.get(GdDoc.GdDocCollectionNames.SOTTODOCUMENTI.toString());
                if (collection != null && ((String)collection).equalsIgnoreCase(GdDoc.GdDocCollectionValues.LOAD.toString())) {
                    log.debug("carico: " + GdDoc.GdDocCollectionNames.SOTTODOCUMENTI.toString() + "...");
                    gdDoc.setSottoDocumenti(caricaSottoDocumenti(dbConn, idGdDoc));
                }
            }
        }
        return gdDoc;
    }

    private static List<Fascicolazione> caricaFascicolazioni(Connection dbConn, SimpleDocument doc, String prefix) throws SQLException {
        IodaFascicolazioniUtilities fascicolazioniUtilities = new IodaFascicolazioniUtilities(doc, prefix);
        return fascicolazioniUtilities.getFascicolazioni(dbConn);
    }
    
    private static List<PubblicazioneIoda> caricaPubblicazioni(Connection dbConn, String idGdDoc) throws SQLException {
        String sqlText = 
                "SELECT  numero_pubblicazione, " +
                        "anno_pubblicazione, " +
                        "data_dal, " +
                        "data_al, " +
                        "pubblicatore, " + 
                        "esecutivita, " + 
                        "data_defissione " +
                        "FROM " + ApplicationParams.getPubblicazioniAlboTableName() + " " +
                        "WHERE id_gddoc = ? " + 
                        "ORDER BY numero_pubblicazione";

        List<PubblicazioneIoda> pubblicazioni = new ArrayList<>();
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, idGdDoc);

            log.debug("eseguo la query: " + ps.toString() + "...");
            ResultSet res = ps.executeQuery();
            while (res.next()) {
                PubblicazioneIoda p = new PubblicazioneIoda();
                p.setAnnoPubblicazione(res.getInt("anno_pubblicazione"));
                p.setDataAl(new DateTime(res.getDate("data_al").getTime()));
                p.setDataDal(new DateTime(res.getDate("data_dal").getTime()));
                p.setNumeroPubblicazione(res.getLong("numero_pubblicazione"));
                p.setPubblicatore(res.getString("pubblicatore"));
                p.setEsecutivita(res.getString("esecutivita"));
                Timestamp dataDefissione = res.getTimestamp("data_defissione");
                if (dataDefissione != null)
                    p.setDataDefissione(new DateTime(dataDefissione.getTime()));
                pubblicazioni.add(p);
            }
        }
        return pubblicazioni;
    }

    public static List<SottoDocumento> caricaSottoDocumenti(Connection dbConn, String idGdDoc) throws SQLException {
        String sqlText =        
                "SELECT  id_sottodocumento, " + 
                        "codice_sottodocumento, " +
                        "tipo_sottodocumento, " +
                        "tipo_firma, " +
                        "principale, " +
                        "nome_sottodocumento, " +
                        "uuid_mongo_pdf, " +
                        "uuid_mongo_firmato, " +
                        "uuid_mongo_originale, " +
                        "mimetype_file_firmato, " +
                        "mimetype_file_originale, " +
                        "da_spedire_pecgw, " +
                        "spedisci_originale_pecgw, " +
                        "dimensione_pdf, " +
                        "dimensione_firmato, " +
                        "dimensione_originale " +
                "FROM " + ApplicationParams.getSottoDocumentiTableName() + " " +
                "WHERE id_gddoc = ?";
        List<SottoDocumento> sottoDocumenti = new ArrayList<>();
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, idGdDoc);
            
            log.debug("eseguo la query: " + ps.toString() + "...");
            ResultSet res = ps.executeQuery();
            log.debug("eseguita");
            while (res.next()) {
                SottoDocumento sd = new SottoDocumento();
                sd.setCodiceSottoDocumento(res.getString("codice_sottodocumento"));
                sd.setTipo(res.getString("tipo_sottodocumento"));
                sd.setTipoFirma(TipoFirma.fromString(res.getString("tipo_firma")));
                sd.setPrincipale(res.getInt("principale") != 0);
                sd.setNome(res.getString("nome_sottodocumento"));
                sd.setUuidMongoPdf(res.getString("uuid_mongo_pdf"));
                sd.setUuidMongoFirmato(res.getString("uuid_mongo_firmato"));
                sd.setUuidMongoOriginale(res.getString("uuid_mongo_originale"));
                sd.setMimeTypeFileFirmato(res.getString("mimetype_file_firmato"));
                sd.setMimeTypeFileOriginale(res.getString("mimetype_file_originale"));
                sd.setDaSpedirePecgw(res.getInt("da_spedire_pecgw") != 0);
                sd.setSpedisciOriginalePecgw(res.getInt("spedisci_originale_pecgw") != 0);
                sd.setDimensioneFilePdf(res.getInt("dimensione_pdf"));
                sd.setDimensioneFileFirmato(res.getInt("dimensione_firmato"));
                sd.setDimensioneFileOriginale(res.getInt("dimensione_originale"));
                
                sottoDocumenti.add(sd);
            }
        }
        return sottoDocumenti;
    }   

    
    public String insertGdDoc(Connection dbConn) throws SQLException, IOException, ServletException, UnsupportedEncodingException, MimeTypeException, IodaDocumentException, IodaFileException {

        String sqlText =
                        "INSERT INTO " + getGdDocTable() + "(" +
                        "id_gddoc, nome_gddoc, tipo_gddoc, " +
                        "data_ultima_modifica, stato_gd_doc, " +
                        "data_gddoc, guid_gddoc, codice_registro, " +
                        "data_registrazione, numero_registrazione, " +
                        "anno_registrazione, oggetto, " +
                        "id_oggetto_origine, tipo_oggetto_origine, " +
                        "codice, nome_struttura_firmatario, applicazione, " +
                        "url_command, id_utente_creazione) " +
                        "VALUES (" +
                        "?, ?, ?, " +
                        "?, ?, " +
                        "?, ?, ?, " +
                        "?, ?, " +
                        "?, ?, " +
                        "?, ?, " +
                        "?, ?, ?, " +
                        "?, ?)";
        
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

            // id_gddoc
            ps.setString(index++, gdDoc.getId());

            // nome_gddoc
            ps.setString(index++, gdDoc.getNome());

            // tipo_gddoc
            ps.setString(index++, gdDoc.isRecord() == null || gdDoc.isRecord() ? "r" : "d");

            // data_ultima_modifica
            Timestamp dataUltimaModifica = (gdDoc.getDataUltimaModifica() != null) ? new Timestamp(gdDoc.getDataUltimaModifica().getMillis()) : null;
            ps.setTimestamp(index++, dataUltimaModifica);

            // stato_gd_doc
            ps.setInt(index++, gdDoc.isVisibile() == null || gdDoc.isVisibile() ? 1 : 0);

            // data_gddoc
            if (gdDoc.getData() == null){
                ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
            }
            else{
                ps.setTimestamp(index++, new Timestamp(gdDoc.getData().getMillis()));
            }

            // guid_gddoc
            ps.setString(index++, gdDoc.getGuid());

            // codice_registro
            ps.setString(index++, gdDoc.getCodiceRegistro());

            // data_registrazione
            Timestamp dataRegistrazione = (gdDoc.getDataRegistrazione() != null) ? new Timestamp(gdDoc.getDataRegistrazione().getMillis()) : null;
            ps.setTimestamp(index++, dataRegistrazione);

            // numero_registrazione
            ps.setString(index++, gdDoc.getNumeroRegistrazione());


            // anno_registrazione
            Integer annoRegistrazione = gdDoc.getAnnoRegistrazione();
            if (annoRegistrazione != null) {
                ps.setInt(index++, annoRegistrazione);
            }
            else
                ps.setNull(index++, Types.INTEGER);

            // oggetto
            ps.setString(index++, gdDoc.getOggetto());
    //
    //        // xml_specifico_parer
    //        ps.setString(index++, gdDoc.getXmlSpecificoParer());
    //
    //        // forza_conservazione
    //        ps.setInt(index++, gdDoc.isForzaConservazione() != null && gdDoc.isForzaConservazione() ? -1 : 0);
    //        
    //        // forza_accettazione
    //        ps.setInt(index++, gdDoc.isForzaAccettazione() != null && gdDoc.isForzaAccettazione() ? -1 : 0);
    //        
    //        // forza_collegamento
    //        ps.setInt(index++, gdDoc.isForzaCollegamento() != null && gdDoc.isForzaCollegamento() ? -1 : 0);

            // id_oggetto_origine
            ps.setString(index++, gdDoc.getIdOggettoOrigine());

            // tipo_oggetto_origine
            ps.setString(index++, gdDoc.getTipoOggettoOrigine());

            // codice
            ps.setString(index++, gdDoc.getCodice());

            // nomeStrutturaFirmatario
            ps.setString(index++, gdDoc.getNomeStrutturaFirmatario());

            // applicazione
            ps.setString(index++, gdDoc.getApplicazione());

            // url_command
            ps.setString(index++, gdDoc.getUrlCommand());
            
            // Vecchio categoria_origine ora id_utente_creazione
            ps.setString(index++, gdDoc.getIdUtenteCreazione());
            
            

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("Documento non inserito");

            // inseririmento delle fascicolazioni
            List<Fascicolazione> fascicolazioni = gdDoc.getFascicolazioni();
            if (fascicolazioni != null && !fascicolazioni.isEmpty()) {
                for (Fascicolazione fascicolazione : fascicolazioni) {
                    insertFascicolazione(dbConn, fascicolazione);
                }
            }

            // inserimento SottoDocumenti
            List<SottoDocumento> sottoDocumenti = gdDoc.getSottoDocumenti();
            if (sottoDocumenti != null && !sottoDocumenti.isEmpty()) {
                for (SottoDocumento sottoDocumento : sottoDocumenti) {
                    insertSottoDocumento(dbConn, sottoDocumento);
                }
            }
            return gdDoc.getGuid();
        }
    }

    /**
     * 
     * @param dbConn
     * @param ps
     * @param collectionData contiene le informazioni che indicano se svuotare le collection
     * @throws SQLException
     * @throws IOException
     * @throws ServletException
     * @throws UnsupportedEncodingException
     * @throws MimeTypeException
     * @throws IodaDocumentException
     * @throws IodaFileException 
     */
    public void updateGdDoc(Connection dbConn, Map<String, Object> collectionData) throws SQLException, IOException, ServletException, UnsupportedEncodingException, MimeTypeException, IodaDocumentException, IodaFileException {
        
        String sqlText = 
                "UPDATE " + getGdDocTable() + " SET " +
                "nome_gddoc = coalesce(?, nome_gddoc), " +
                "tipo_gddoc = coalesce(?, tipo_gddoc), " +
                "data_ultima_modifica = coalesce(?, data_ultima_modifica), " +
                "stato_gd_doc = coalesce(?, stato_gd_doc), " +
                "codice_registro = coalesce(?, codice_registro), " +
                "data_registrazione = coalesce(?, data_registrazione), " +
                "numero_registrazione = coalesce(?, numero_registrazione), " +
                "anno_registrazione = coalesce(?, anno_registrazione), " +
                "oggetto = coalesce(?, oggetto), " +
                "nome_struttura_firmatario = coalesce(?, nome_struttura_firmatario), " +
                "applicazione = coalesce(?, applicazione), " +
                "url_command = coalesce(?, url_command), " +
                "id_utente_creazione = coalesce(?, id_utente_creazione) " + 
                "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ? " +
                "returning id_gddoc, guid_gddoc";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

            // nome_gddoc
            ps.setString(index++, gdDoc.getNome());

            // tipo_gddoc
            if (gdDoc.isRecord() != null)
                ps.setString(index++, gdDoc.isRecord() ? "r" : "d");
            else
                ps.setNull(index++, Types.VARCHAR);

            // data_ultima_modifica
            Timestamp dataUltimaModifica = (gdDoc.getDataUltimaModifica() != null) ? new Timestamp(gdDoc.getDataUltimaModifica().getMillis()) : null;
            if (dataUltimaModifica != null)
                ps.setTimestamp(index++, dataUltimaModifica);
            else
                ps.setNull(index++, Types.TIMESTAMP);

            // stato_gd_doc
            if (gdDoc.isVisibile() != null)
                ps.setInt(index++, gdDoc.isVisibile() ? 1 : 0);
            else
                ps.setNull(index++, Types.INTEGER);

            // codice_registro
            ps.setString(index++, gdDoc.getCodiceRegistro());

            // data_registrazione
            Timestamp dataRegistrazione = (gdDoc.getDataRegistrazione() != null) ? new Timestamp(gdDoc.getDataRegistrazione().getMillis()) : null;
            if (dataRegistrazione != null)
                ps.setTimestamp(index++, dataRegistrazione);
            else
                ps.setNull(index++, Types.TIMESTAMP);

            // numero_registrazione
            ps.setString(index++, gdDoc.getNumeroRegistrazione());

            // anno_registrazione
            Integer annoRegistrazione = gdDoc.getAnnoRegistrazione();
            if (annoRegistrazione != null) {
                ps.setInt(index++, annoRegistrazione);
            }
            else
                ps.setNull(index++, Types.INTEGER);

            // oggetto
            ps.setString(index++, gdDoc.getOggetto());

    //        // xml_specifico_parer
    //        ps.setString(index++, gdDoc.getXmlSpecificoParer());
    //
    //        // forza_conservazione
    //        if (gdDoc.isForzaConservazione() != null)
    //            ps.setInt(index++, gdDoc.isForzaConservazione() ? -1 : 0);
    //        else
    //            ps.setNull(index++, Types.INTEGER);
    //        
    //        // forza_accettazione
    //        if (gdDoc.isForzaAccettazione() != null)
    //            ps.setInt(index++, gdDoc.isForzaAccettazione() ? -1 : 0);
    //        else
    //            ps.setNull(index++, Types.INTEGER);
    //        
    //        // forza_collegamento
    //        if (gdDoc.isForzaCollegamento() != null)
    //            ps.setInt(index++, gdDoc.isForzaCollegamento() ? -1 : 0);
    //        else
    //            ps.setNull(index++, Types.INTEGER);

            // nome_struttura_firmatario
            ps.setString(index++, gdDoc.getNomeStrutturaFirmatario());

            // applicazione
            ps.setString(index++, gdDoc.getApplicazione());
            
            // url_command
            ps.setString(index++, gdDoc.getUrlCommand());
            
            // Vecchio categoria_origine ora id_utente_creazione
            ps.setString(index++, gdDoc.getIdUtenteCreazione());

            // id_oggetto_origine
            ps.setString(index++, gdDoc.getIdOggettoOrigine());

            // tipo_oggetto_origine
            ps.setString(index++, gdDoc.getTipoOggettoOrigine());
            
            

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();
            if (!result.next())
                throw new SQLException("Documento non trovato");
            gdDoc.setId(result.getString(1));
            gdDoc.setGuid(result.getString(2));
        }
        // fascicolazioni
        // azzeramento della collection, se richiesto
        if (collectionData != null) {
            Object collection = collectionData.get(GdDoc.GdDocCollectionNames.FASCICOLAZIONI.toString());
            if (collection != null && ((String)collection).equalsIgnoreCase(GdDoc.GdDocCollectionValues.CLEAR.toString()))
                deleteFascicolazioni(dbConn);
        }
        // gestione delle fascicolazioni
        List<Fascicolazione> fascicolazioni = gdDoc.getFascicolazioni();
        if (fascicolazioni != null && !fascicolazioni.isEmpty()) {
            for (Fascicolazione fascicolazione : fascicolazioni) {
                switch (fascicolazione.getTipoOperazione()) {
                case INSERT:
                    insertFascicolazione(dbConn, fascicolazione);
                    break;
                case UPDATE:
                    updateFascicolazione(dbConn, fascicolazione);
                    break;
                case DELETE:
                    deleteFascicolazione(dbConn, fascicolazione);
                    break;
                default:
                    throw new IodaDocumentException("operazione non consentita");
                }
            }
        }

        // SottoDocumenti
        // azzeramento della collection, se richiesto
        if (collectionData != null) {
            Object collection = collectionData.get(GdDoc.GdDocCollectionNames.SOTTODOCUMENTI.toString());
            if (collection != null && ((String)collection).equalsIgnoreCase(GdDoc.GdDocCollectionValues.CLEAR.toString()))
                deleteSottoDocumenti(dbConn);
        }
        // gestione deli sottodocumenti
        List<SottoDocumento> sottoDocumenti = gdDoc.getSottoDocumenti();
        if (sottoDocumenti != null && !sottoDocumenti.isEmpty()) {
            for (SottoDocumento sottoDocumento : sottoDocumenti) {
                switch (sottoDocumento.getTipoOperazione()) {
                case INSERT:
                    insertSottoDocumento(dbConn, sottoDocumento);
                    break;
                case DELETE:
                    deleteSottoDocumento(dbConn, sottoDocumento);
                    break;
                default:
                    throw new IodaDocumentException("operazione non consentita");
                }
            }
        }
        
        // inserimento Pubblicazioni
        // le pullicazioni non possono essere cancellate
        List<PubblicazioneIoda> pubblicazioni = gdDoc.getPubblicazioni();
        if (pubblicazioni != null && !pubblicazioni.isEmpty()) {
            for (PubblicazioneIoda pubblicazione : pubblicazioni) {
                if (pubblicazione.getTipoOperazione() == Document.DocumentOperationType.INSERT)
                    insertPubblicazione(dbConn, pubblicazione);
                else if (pubblicazione.getTipoOperazione() == Document.DocumentOperationType.UPDATE)
                    throw new IodaDocumentException("operazione non ancora implementata");
                else
                    throw new IodaDocumentException("operazione non consentita");
            }
        }
    }

    public void deleteGdDoc(Connection dbConn) throws SQLException, IodaDocumentException{
//        String sqlText = 
//                "SELECT numero_registrazione " +
//                "FROM " + getGdDocTable() + " " +
//                "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
//        
//        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
//            ps.setString(1, gdDoc.getIdOggettoOrigine());
//            ps.setString(2, gdDoc.getTipoOggettoOrigine());
//            ResultSet res = ps.executeQuery();
//            if (!res.next())
//                throw new SQLException("documento non trovato");
//            String numeroRegistrazione = res.getString(1);
//            if (numeroRegistrazione != null && !numeroRegistrazione.equals(""))
//                throw new IodaDocumentException("impossibile eliminare un document registrato");
//        }

        boolean accessoRepository = UtilityFunctions.hasAccessoRepository(dbConn, gdDoc.getApplicazione());
        if (!accessoRepository) {
            String sqlText = 
                  "SELECT s.uuid_mongo_originale, s.uuid_mongo_pdf, s.uuid_mongo_firmato " +
                  "FROM " + getSottoDocumentiTable() + " s INNER JOIN " + getGdDocTable() + " g ON s.id_gddoc = g.id_gddoc " +
                  "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
            try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                ps.setString(1, gdDoc.getIdOggettoOrigine());
                ps.setString(2, gdDoc.getTipoOggettoOrigine());
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    String uuid = res.getString(1);
                    if (uuid != null && !uuid.equals(""))
                        uuidsToDelete.add(uuid);

                    uuid = res.getString(2);
                    if (uuid != null && !uuid.equals(""))
                        uuidsToDelete.add(uuid);

                    uuid = res.getString(3);
                    if (uuid != null && !uuid.equals(""))
                        uuidsToDelete.add(uuid);
                }
            }
        }

        String sqlText =
                "DELETE " +
                "FROM " + getGdDocTable() + " " +
                "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, gdDoc.getIdOggettoOrigine());
            ps.setString(2, gdDoc.getTipoOggettoOrigine());
            int ris = ps.executeUpdate();

            if (ris > 0)
                if (uuidsToDelete != null && !uuidsToDelete.isEmpty())
                    deleteAllMongoFileToDelete();
        }
    }   
    
    public void insertSottoDocumento(Connection dbConn, SottoDocumento sd) throws SQLException, IOException, ServletException, UnsupportedEncodingException, MimeTypeException, IodaDocumentException, IodaFileException {

        sd.setPrefissoApplicazioneOrigine(prefixIds);
        JSONObject newIds = getNextIndeId();
        String idSottoDocumento = (String) newIds.get(INDE_DOCUMENT_ID_PARAM_NAME);
        String guidSottoDocumento = (String) newIds.get(INDE_DOCUMENT_GUID_PARAM_NAME);

        String mongoPath = getMongoPath();
        
        // se esiste già un sotto documento con lo stesso codice, non lo inserisco
        Boolean canInsertSottoDocumento = canInsertSottoDocumento(dbConn, sd.getCodiceSottoDocumento());
        if(canInsertSottoDocumento == false){
            return;
        }

        String sqlText = 
                "INSERT INTO " + getSottoDocumentiTable() + "(" +
                "id_sottodocumento, guid_sottodocumento, id_gddoc, nome_sottodocumento, " +
                "uuid_mongo_pdf, dimensione_pdf, convertibile_pdf, " +
                "uuid_mongo_originale, dimensione_originale, " +
                "uuid_mongo_firmato, dimensione_firmato, " +
                "principale, tipo_sottodocumento, tipo_firma, " +
                "mimetype_file_originale, mimetype_file_firmato, " +
                "codice_sottodocumento, " +
                "da_spedire_pecgw, spedisci_originale_pecgw)" +
                "VALUES (" +
                "?, ?, ?, ?, " +
                "?, ?, ?, " +
                "?, ?, " +
                "?, ?, " +
                "?, ?, ?, " +
                "?, ?, " +
                "?, " +
                "?, ?)";
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

            ps.setString(index++, idSottoDocumento);
            ps.setString(index++, guidSottoDocumento);
            ps.setString(index++, gdDoc.getId());
            ps.setString(index++, sd.getNome());

            if (sd.getUuidMongoPdf() != null && !sd.getUuidMongoPdf().equals("")) {
                int fileLenght = mongo.getSizeByUuid(sd.getUuidMongoPdf()).intValue();
                ps.setString(index++, sd.getUuidMongoPdf());
                ps.setInt(index++, fileLenght);
                ps.setInt(index++,  -1);
            }
            else {
                ps.setString(index++, null);
                ps.setNull(index++, Types.INTEGER);
                ps.setInt(index++,  0);
            }

            if (sd.getUuidMongoOriginale() != null && !sd.getUuidMongoOriginale().equals("")) {
                int fileLenght = mongo.getSizeByUuid(sd.getUuidMongoOriginale()).intValue();
                ps.setString(index++, sd.getUuidMongoOriginale());
                ps.setInt(index++, fileLenght);
            }
            else if (sd.getNomePartFileOriginale() != null) {
                IodaFile uploadedMongoFile = uploadOnMongo(sd.getNomePartFileOriginale(), mongoPath);
                String uuid = uploadedMongoFile.getUuid();
                sd.setUuidMongoOriginale(uuid);
                sd.setMimeTypeFileOriginale(uploadedMongoFile.getMimeType());
                sd.setNomeFileOriginale(uploadedMongoFile.getFileName());
                int fileLenght = mongo.getSizeByUuid(uuid).intValue();
                ps.setString(index++, uuid);
                ps.setInt(index++, fileLenght);
            }
            else {
                ps.setString(index++, null);
                ps.setNull(index++, Types.INTEGER);
            }

            if (sd.getUuidMongoFirmato() != null && !sd.getUuidMongoFirmato().equals("")) {
                int fileLenght = mongo.getSizeByUuid(sd.getUuidMongoFirmato()).intValue();
                ps.setString(index++, sd.getUuidMongoFirmato());
                ps.setInt(index++, fileLenght);
            }
            else if (sd.getNomePartFileFirmato() != null) {
                IodaFile uploadedMongoFile = uploadOnMongo(sd.getNomePartFileFirmato(), mongoPath);
                String uuid = uploadedMongoFile.getUuid();
                sd.setUuidMongoFirmato(uuid);
                sd.setMimeTypeFileFirmato(uploadedMongoFile.getMimeType());
                sd.setNomeFileFirmato(uploadedMongoFile.getFileName());
                int fileLenght = mongo.getSizeByUuid(uuid).intValue();
                ps.setString(index++, uuid);
                ps.setInt(index++, fileLenght);
            }
            else {
                ps.setString(index++, null);
                ps.setNull(index++, Types.INTEGER);
            }

            ps.setInt(index++, sd.isPrincipale() == null || !sd.isPrincipale() ? 0: -1);
            ps.setString(index++, sd.getTipo());

            if (sd.getTipoFirma() != null)
                ps.setString(index++, sd.getTipoFirma().getKey());
            else
                ps.setString(index++, null);

            if (sd.getUuidMongoOriginale() != null && sd.getMimeTypeFileOriginale() == null)
                throw new IodaDocumentException("il mimeType del file originale è nullo");
            ps.setString(index++, sd.getMimeTypeFileOriginale());

            if (sd.getUuidMongoFirmato() != null && sd.getMimeTypeFileFirmato() == null)
                throw new IodaDocumentException("il mimeType del file firmato è nullo");
            ps.setString(index++, sd.getMimeTypeFileFirmato());

            ps.setString(index++, sd.getCodiceSottoDocumento());

            ps.setInt(index++, sd.isDaSpedirePecgw() ? -1 : 0);
            ps.setInt(index++, sd.isSpedisciOriginalePecgw() ? -1 : 0);

            // aggiungo in una lista i sottodocumenti potenzialmente convertibili in pdf (cioè quelli per cui, non mi è stato passato l'uuid del file in pdf), il controllo
            // per verificare se la conversione è supportata verrà fatto successivamente
            if (MediaType.parse(sd.getMimeTypeFileOriginale()) != (Detector.MEDIA_TYPE_APPLICATION_PDF) && (sd.getUuidMongoPdf() == null || sd.getUuidMongoPdf().equals(""))) {
                toConvert.add(sd);
            }

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("SottoDocumento non inserito");
        }
    }
    
    public void deleteSottoDocumento(Connection dbConn, SottoDocumento sd) throws SQLException {
        sd.setPrefissoApplicazioneOrigine(prefixIds);

        // leggo dalla tabella delle applicazioni se l'applicazione ha l'accesso al repository documentale
        boolean accessoRepository = UtilityFunctions.hasAccessoRepository(dbConn, gdDoc.getApplicazione());
        if (!accessoRepository) {

            String sqlText = 
                    "SELECT uuid_mongo_originale, uuid_mongo_pdf, uuid_mongo_firmato " +
                    "FROM " + getSottoDocumentiTable() + " " +
                    "WHERE codice_sottodocumento = ?";

            try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                ps.setString(1, sd.getCodiceSottoDocumento());
                ResultSet res = ps.executeQuery();

                if (!res.next())
                    throw new SQLException("SottoDocumento non trovato");
                do {
                    String uuid = res.getString(1);
                    if (uuid != null && !uuid.equals(""))
                        uuidsToDelete.add(uuid);

                    uuid = res.getString(2);
                    if (uuid != null && !uuid.equals(""))
                        uuidsToDelete.add(uuid);

                    uuid = res.getString(3);
                    if (uuid != null && !uuid.equals(""))
                        uuidsToDelete.add(uuid);
                }
                while (res.next());
            }
        }

        String sqlText =   "DELETE FROM " + getSottoDocumentiTable() + " " +
                    "WHERE codice_sottodocumento = ?";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, sd.getCodiceSottoDocumento());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0)
                throw new SQLException("SottoDocumento non trovato");
        }

        if (uuidsToDelete != null && !uuidsToDelete.isEmpty())
            deleteAllMongoFileToDelete();
    }

    public void deleteSottoDocumenti(Connection dbConn) throws SQLException {

        // leggo dalla tabella delle applicazioni se l'applicazione ha l'accesso al repository documentale
        boolean accessoRepository = UtilityFunctions.hasAccessoRepository(dbConn, gdDoc.getApplicazione());
        if (!accessoRepository) {

            String sqlText = 
                    "SELECT uuid_mongo_originale, uuid_mongo_pdf, uuid_mongo_firmato " +
                    "FROM " + getSottoDocumentiTable() + " " +
                    "WHERE id_gddoc = ?";

            try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
                ps.setString(1, gdDoc.getId());
                ResultSet res = ps.executeQuery();

                if (res.next()) {
                    do {
                        String uuid = res.getString(1);
                        if (uuid != null && !uuid.equals(""))
                            uuidsToDelete.add(uuid);

                        uuid = res.getString(2);
                        if (uuid != null && !uuid.equals(""))
                            uuidsToDelete.add(uuid);

                        uuid = res.getString(3);
                        if (uuid != null && !uuid.equals(""))
                            uuidsToDelete.add(uuid);
                    }
                    while (res.next());
                }
            }
        }

        String sqlText =   "DELETE FROM " + getSottoDocumentiTable() + " " +
                    "WHERE id_gddoc = ?";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, gdDoc.getId());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
        }

        if (uuidsToDelete != null && !uuidsToDelete.isEmpty())
            deleteAllMongoFileToDelete();
    }
    
    private IodaFile uploadOnMongo(String filePartName, String mongoPath) throws IOException, IodaFileException, ServletException, SQLException, UnsupportedEncodingException, MimeTypeException {
        InputStream is = null;
        FileOutputStream fos = null;
        File tempFile = null;
        try {
            Part filePart = request.getPart(filePartName);
            if (filePart == null)
                throw new IodaFileException("part \"" + filePartName + "\" non trovata");
            is = filePart.getInputStream();
            tempFile = File.createTempFile("received_file_", null);
            fos = new FileOutputStream(tempFile);
            IOUtils.copy(is, fos);
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(fos);
            
            String mimeType = detector.getMimeType(tempFile.getAbsolutePath());
            if (SupportedFile.isSupported(ApplicationParams.getSupportedFileList(), mimeType)) {
                String uuid = mongo.put(tempFile, filePart.getSubmittedFileName(), mongoPath, false);
                uploadedUuids.add(uuid);
                return new IodaFile(uuid, filePart.getSubmittedFileName() ,mimeType);
            }
            else {
                throw new IodaFileException("il file: \"" + filePart.getSubmittedFileName() + "\" contenuto nella part \"" + filePartName + "\" non è accettato");
            }
        }
        finally {
            if (tempFile != null)
                tempFile.delete();
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(fos);
        }
    }

    public static void UpdatePubblicazioneById(Connection dbConn, long idPubblicaizone, PubblicazioneIoda p) throws NamingException, ServletException, SQLException{

        String sqlText = 
                "UPDATE " + ApplicationParams.getPubblicazioniAlboTableName() + " SET " +
                "numero_pubblicazione = coalesce(?, numero_pubblicazione), " +
                "anno_pubblicazione = coalesce(?, anno_pubblicazione), " +
                "pubblicatore = coalesce(?, pubblicatore), " +
                "data_defissione = coalesce(?, data_defissione) " +
                "WHERE id = ? ";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

             // numero
            ps.setLong(index++, p.getNumeroPubblicazione());

            // anno
            ps.setInt(index++, p.getAnnoPubblicazione());

            // pubblicatore
            ps.setString(index++, p.getPubblicatore());

            //data defissione
            if (p.getDataDefissione() != null)
                ps.setTimestamp(index++, new Timestamp(p.getDataDefissione().getMillis()));
            else
                ps.setNull(index++, Types.TIMESTAMP);

            // id
            ps.setLong(index++, idPubblicaizone);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int rowsUpdated = ps.executeUpdate();
            log.debug("eseguita");
            if (rowsUpdated == 0)
                throw new SQLException("Documento non trovato");
            else if (rowsUpdated > 1)
                log.fatal("troppe righe aggiornate, aggiornate " + rowsUpdated + " righe, dovrebbe essere una");
        }
    }
    
    public static void UpdatePubblicazioneByNumeroAndAnno(Connection dbConn, long numeroPubblicazione, int annoPubblicazione, PubblicazioneIoda p) throws NamingException, ServletException, SQLException{

        String sqlText = 
                "UPDATE " + ApplicationParams.getPubblicazioniAlboTableName() + " SET " +
                "data_defissione = coalesce(?, data_defissione), " +
                "pubblicatore = coalesce(?, pubblicatore) " +
                "WHERE numero_pubblicazione = ? AND anno_pubblicazione = ?";
        
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

            //data defissione
            if (p.getDataDefissione() != null)
                ps.setTimestamp(index++, new Timestamp(p.getDataDefissione().getMillis()));
            else
                ps.setNull(index++, Types.TIMESTAMP);

            // pubblicatore
            ps.setString(index++, p.getPubblicatore());

            // numero
            ps.setLong(index++, numeroPubblicazione);

            // anno
            ps.setInt(index++, annoPubblicazione);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int rowsUpdated = ps.executeUpdate();
            log.debug("eseguita");
            if (rowsUpdated == 0)
                throw new SQLException("Documento non trovato");
            else if (rowsUpdated > 1)
                log.fatal("troppe righe aggiornate, aggiornate " + rowsUpdated + " righe, dovrebbe essere una");
        }
    }
    
    public void deleteAllMongoFileUploaded() {
        uploadedUuids.stream().forEach((uuid) -> {
            mongo.delete(uuid);
        });
        uploadedUuids.clear();
    }

    public void deleteAllMongoFileToDelete() {
        uuidsToDelete.stream().forEach((uuid) -> {
            mongo.delete(uuid);
        });
        uuidsToDelete.clear();
    }

    private void getIndeId() throws IOException, MalformedURLException, SendHttpMessageException {
            
            // contruisco la mappa dei parametri per la servlet (va passato solo un parametro che è il numero di id da generare)
            Map<String, byte[]> params = new HashMap<>();
            
            // il numero di id è ottenuto dal numero dei SottoDocumenti + il GdDoc
            int idNumber = 1;
             if (gdDoc.getFascicolazioni()!= null)
                idNumber += gdDoc.getFascicolazioni().size();
            if (gdDoc.getSottoDocumenti() != null)
                idNumber += gdDoc.getSottoDocumenti().size();
            params.put(GENERATE_INDE_NUMBER_PARAM_NAME, String.valueOf(idNumber).getBytes());
            
            String res = UtilityFunctions.sendHttpMessage(getIndeIdServletUrl, null, null, params, "POST", null);
            
            // la servlet torna un JsonArray contente idNumber elementi, ognuno dei quali è una coppia (id, guid)
            indeId = (JSONArray) JSONValue.parse(res);
    }

    public String getGdDocTable() {
        return gdDocTable;
    }

    public String getSottoDocumentiTable() {
        return sottoDocumentiTable;
    }

    public String getFascicoliTable() {
        return fascicoliTable;
    }

    public String getFascicoliGdDocTable() {
        return fascicoliGdDocTable;
    }

    public void convertPdf() throws SQLException, NamingException {
        Connection dbConn = null;
        PreparedStatement ps = null;
        try {
            dbConn = UtilityFunctions.getDBConnection();
            for (SottoDocumento sd : toConvert) {
                try {
                    String uuidToConvert = null;
                    String mimeType = null;
                    String fileName = null;
                    if (sd.getUuidMongoOriginale() != null && !sd.getUuidMongoOriginale().equals("")) {
                        uuidToConvert = sd.getUuidMongoOriginale();
                        mimeType = sd.getMimeTypeFileOriginale();
                        fileName = sd.getNomeFileOriginale();
                    }
                    else if (sd.getUuidMongoFirmato() != null && !sd.getUuidMongoFirmato().equals("")) {
                        uuidToConvert = sd.getUuidMongoFirmato();
                        mimeType = sd.getMimeTypeFileFirmato();
                        fileName = sd.getNomeFileFirmato();
                    }
                    if (uuidToConvert != null) {
                        String pdfMongoPath = getMongoPath() + "/" + UtilityFunctions.removeExtensionFromFileName(fileName) + "_" + UtilityFunctions.getExtensionFromFileName(fileName) + "_convertito_pdf.pdf";
                        String convertedUuid = UtilityFunctions.convertPdf(uuidToConvert, pdfMongoPath, mimeType);
                        String sqlText = 
                            "UPDATE " + getSottoDocumentiTable() + " " +
                            "SET " +
                            "uuid_mongo_pdf = ?, " +
                            "convertibile_pdf = ? " +
                            "WHERE codice_sottodocumento = ?";
                            ps = dbConn.prepareStatement(sqlText);

                            ps.setString(1, convertedUuid);
                            ps.setInt(2, -1);
                            ps.setString(3, sd.getCodiceSottoDocumento());
                            
                            log.debug("eseguo la query: " + ps.toString() + "...");
                            int res = ps.executeUpdate();
                            if (res == 0)
                                throw new SQLException("sottoDocumento: " + sd.getCodiceSottoDocumento() + " non trovato");
                    }
                }
                catch (Exception ex) {
                    log.error("errore nella conversione in pdf dei sottodocumenti: ", ex);
                }
            }
        }
        finally {
            if (ps != null)
                ps.close();
            if (dbConn != null)
                dbConn.close();
        }
    } 
    
    
    private Boolean canInsertSottoDocumento(Connection dbConn, String idGdDoc) throws SQLException {
        String sqlText =
                        "SELECT codice_sottodocumento " +
                        "FROM " + sottoDocumentiTable + " " +
                        "WHERE codice_sottodocumento = ? ";
        PreparedStatement ps = null;
        try {
            String codiceSottoDocumento;
            ps = dbConn.prepareStatement(sqlText);
            ps.setString(1, idGdDoc);
            ResultSet res = ps.executeQuery();
            if (res.next() == false)
                return true;
            else 
                codiceSottoDocumento = res.getString(1);
            return false;
        }
        finally {
            try {
                ps.close();
            }
            catch (Exception ex) {
            }
        }
    }
    
    public String registraDocumento(Connection dbConn, PreparedStatement ps, ServletContext context, String guid, String codiceRegistro) throws ServletException, SQLException {
        Registro r = Registro.getRegistro(codiceRegistro, dbConn);
        return SetDocumentNumber.setNumber(dbConn, context, guid, r.getSequenzaAssociata());
    }

    public static ArrayList<GdDoc> getGdDocsDaPubblicare(Connection dbConn) throws SQLException {          
        ArrayList<GdDoc> listaGddocs = new ArrayList<>();

        String sqlText = "SELECT g.id_gddoc, " +
                        "g.applicazione, " +
                        "g.codice_registro, " +
                        "g.data_gddoc, " +
                        "g.data_registrazione, " +
                        "g.id_oggetto_origine, " +
                        "g.nome_struttura_firmatario, " +
                        "g.numero_registrazione, " +
                        "g.oggetto, " +
                        "g.tipo_oggetto_origine, " +
                        "g.stato_gd_doc, " + 
                        "json_agg(row_to_json(p.*)) AS pubblicazioni " +
                        "FROM " + ApplicationParams.getGdDocsTableName() + " g " +
                        "INNER JOIN " + ApplicationParams.getPubblicazioniAlboTableName() + " p on g.id_gddoc = p.id_gddoc  " +
                        "WHERE p.numero_pubblicazione IS NULL " +
                        "AND p.anno_pubblicazione IS NULL " +
                        "AND g.numero_registrazione IS NOT NULL " +
                        "AND g.tipo_gddoc = ? " +
                        "GROUP BY g.id_gddoc, g.applicazione, g.codice, g.codice_registro, g.data_gddoc, g.data_registrazione, g.data_ultima_modifica, " +
                        "g.id_oggetto_origine, g.nome_gddoc, g.nome_struttura_firmatario, g.numero_registrazione, g.oggetto, g.tipo_gddoc, g.tipo_oggetto_origine, " +
                        "g.stato_gd_doc, p.id " +
                        "ORDER BY p.id ";

        PreparedStatement s = null;

        try {
            s = dbConn.prepareStatement(sqlText);
            s.setString(1, "r");
            String query = s.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = s.executeQuery();
            log.debug("eseguita");
            //Inizio a ciclare sugli elementi trovati....
            while(result.next()){
                GdDoc gdDocToCreate = new GdDoc();
                gdDocToCreate.setId(result.getString("id_gddoc"));
                gdDocToCreate.setApplicazione(result.getString("applicazione"));
                gdDocToCreate.setCodiceRegistro(result.getString("codice_registro"));
                gdDocToCreate.setData(new DateTime(result.getTimestamp("data_gddoc").getTime()));
                gdDocToCreate.setDataRegistrazione(new DateTime(result.getTimestamp("data_registrazione").getTime()));
                gdDocToCreate.setIdOggettoOrigine(result.getString("id_oggetto_origine"));
                gdDocToCreate.setNomeStrutturaFirmatario(result.getString("nome_struttura_firmatario"));
                gdDocToCreate.setNumeroRegistrazione(result.getString("numero_registrazione"));
                gdDocToCreate.setOggetto(result.getString("oggetto"));
               // gdDocToCreate.setRecord(result.getString("tipo_gddoc") == null || result.getString("tipo_gddoc").equalsIgnoreCase("r"));
                gdDocToCreate.setTipoOggettoOrigine(result.getString("tipo_oggetto_origine"));
                gdDocToCreate.setVisibile(result.getInt("stato_gd_doc") != 0); 
                
                //Questo campo è un array di elementi Json
                String elencoPubblicazioniString  = result.getString("pubblicazioni");      
                JSONArray jsonArray = (JSONArray) JSONValue.parse(elencoPubblicazioniString);

                ArrayList<PubblicazioneIoda> pubblicazioniList = new ArrayList<>();
                for (int i = 0; i < jsonArray.size(); i++) {
                   
                    JSONObject pubblicazioneJson = (JSONObject) jsonArray.get(i);
                    
                    PubblicazioneIoda pubblicazione = new PubblicazioneIoda();    
                    pubblicazione.setId((Long) pubblicazioneJson.get("id"));
                    pubblicazione.setNumeroPubblicazione((Long) pubblicazioneJson.get("numero_pubblicazione"));
                    pubblicazione.setAnnoPubblicazione((Integer) pubblicazioneJson.get("anno_pubblicazione"));
                    pubblicazione.setDataDal(DateTime.parse((String) pubblicazioneJson.get("data_dal")));
                    pubblicazione.setDataAl(DateTime.parse((String) pubblicazioneJson.get("data_al")));
                    String data_esecutivita = (String) pubblicazioneJson.get("data_esecutivita");
                    if (data_esecutivita != null && !data_esecutivita.isEmpty())
                        pubblicazione.setDataEsecutivita(DateTime.parse(data_esecutivita));
                    pubblicazione.setEsecutivita((String) pubblicazioneJson.get("esecutivita"));
                    pubblicazione.setEsecutiva(((Long) pubblicazioneJson.get("esecutiva")) != 0);
                    pubblicazione.setPubblicatore((String) pubblicazioneJson.get("pubblicatore"));
                    //Aggiungo la pubblicazione appena creata
                    pubblicazioniList.add(pubblicazione);
                }

                //Setto le pubblicazioni al gddoc
                gdDocToCreate.setPubblicazioni(pubblicazioniList);

                //Aggiungo il GDDOC alla lista dei Gddoc
                listaGddocs.add(gdDocToCreate);
                
            } //END WHILE
        }
        finally {
            if(s != null) {
                try {
                    s.close();
                }
                catch (SQLException ex) {
                }
            }
        }
      
        
        return listaGddocs;
    }
}