package it.bologna.ausl.bds_tools.ioda.utils;

import com.mongodb.MongoException;
import it.bologna.ausl.bds_tools.ApplicationParams;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.SupportedFile;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.Document;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.GdDoc;
import it.bologna.ausl.ioda.iodaobjectlibrary.SottoDocumento;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.ioda.iodaoblectlibrary.exceptions.IodaFileException;
import it.bologna.ausl.mimetypeutility.Detector;
import it.bologna.ausl.mongowrapper.MongoWrapper;
import it.bologna.ausl.mongowrapper.MongoWrapperException;
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
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
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
import org.apache.tika.mime.MimeTypeException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author gdm
 */
public class IodaDocumentUtilities {
private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(IodaDocumentUtilities.class);
    
public static final String INDE_DOCUMENT_ID_PARAM_NAME = "document_id";
public static final String INDE_DOCUMENT_GUID_PARAM_NAME = "document_guid";
private static final String GENERATE_INDE_NUMBER_PARAM_NAME = "generateidnumber";

HttpServletRequest request;
private String idApplicazione;
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
private JSONObject gdDocIndeId;
private Detector detector;


private final List<SottoDocumento> toConvert = new ArrayList<SottoDocumento>();
private final List<String> uploadedUuids = new ArrayList<String>();

    private IodaDocumentUtilities(ServletContext context, Document.DocumentOperationType operation, String idApplicazione) throws UnknownHostException, MongoException, MongoWrapperException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.gdDocTable = context.getInitParameter("GdDocsTableName");
        String mongoUri = ApplicationParams.getMongoUri();
        this.mongo = new MongoWrapper(mongoUri);
        this.sottoDocumentiTable = context.getInitParameter("SottoDocumentiTableName");

        if (operation != Document.DocumentOperationType.DELETE) {
            this.idApplicazione = idApplicazione;
            this.detector = new Detector();
            this.indeIdIndex = 0; 
            this.mongoParentPath = context.getInitParameter("UploadGdDocMongoPath");
            this.getIndeIdServletUrl = context.getInitParameter("getindeidurl" + ApplicationParams.getServerId());
            this.fascicoliTable = context.getInitParameter("FascicoliTableName");
            this.fascicoliGdDocTable = context.getInitParameter("FascicoliGdDocsTableName");
        }
    }

    public IodaDocumentUtilities(ServletContext context, HttpServletRequest request, Document.DocumentOperationType operation, String idApplicazione) throws UnknownHostException, MongoException, MongoWrapperException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(context, operation, idApplicazione);
        this.request = request;
        InputStream gdDocIs = null;
        try {
            Part part = request.getPart("document");
            gdDocIs = part.getInputStream();
        }
        catch (Exception ex) {
            throw new IodaDocumentException("document non trovato");
        }
 
        try {
            this.gdDoc = GdDoc.getGdDoc(gdDocIs, operation);   
            this.gdDoc.setPrefissoApplicazioneOrigine(this.idApplicazione);
        }
        finally {
            IOUtils.closeQuietly(gdDocIs);
        }
        if (operation != Document.DocumentOperationType.DELETE) {
            getIndeId();
            this.gdDocIndeId = getNextIndeId();
        }
    }

    public IodaDocumentUtilities(ServletContext context, GdDoc gdDoc, Document.DocumentOperationType operation, String idApplicazione) throws UnknownHostException, MongoException, MongoWrapperException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(context, operation, idApplicazione);
        this.gdDoc = gdDoc;
        this.gdDoc.setPrefissoApplicazioneOrigine(this.idApplicazione);

        if (operation != Document.DocumentOperationType.DELETE) {
            getIndeId();
            this.gdDocIndeId = getNextIndeId();
        }
    }

    private JSONObject getNextIndeId() {
        JSONObject currentId = (JSONObject) indeId.get(indeIdIndex++);
        return currentId;
    }

    public String getMongoParentPath() {
        return mongoParentPath;
    }
    
    public String getMongoPath() {
        String guidGdDoc = (String) gdDocIndeId.get(INDE_DOCUMENT_GUID_PARAM_NAME);
        // mongopath: nome[_numeroRegistrazione]_guidGdDoc
        return mongoParentPath + "/" + gdDoc.getNome() + (gdDoc.getNumeroRegistrazione() != null ? "_" + gdDoc.getNumeroRegistrazione() : "") + "_" + guidGdDoc;
    }

    public void insertInFascicolo(Connection dbConn, PreparedStatement ps, Fascicolo fascicolo) throws SQLException {
        String idFascicolo = getIdFascicolo(dbConn, ps, fascicolo);
        
        JSONObject newIds = getNextIndeId();
        String idFascicoloGdDoc = (String) newIds.get(INDE_DOCUMENT_ID_PARAM_NAME);
        String guidFascicoloGdDoc = (String) newIds.get(INDE_DOCUMENT_GUID_PARAM_NAME);

        String idGdDoc = (String) gdDocIndeId.get(INDE_DOCUMENT_ID_PARAM_NAME);
        
        String sqlText = 
                "INSERT INTO " + getFascicoliGdDocTable() + "(" +
                "id_fascicolo_gddoc, id_gddoc, id_fascicolo, data_assegnazione) " +
                "VALUES (?, ?, ?, ?)";
        ps = dbConn.prepareStatement(sqlText);
        int index = 1;
        ps.setString(index++, idFascicoloGdDoc);
        ps.setString(index++, idGdDoc);
        ps.setString(index++, idFascicolo);
        ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        
        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        int result = ps.executeUpdate();
        if (result <= 0)
            throw new SQLException("Fascicolo non inserito");
    }
    
    public void deleteFromFascicolo(Connection dbConn, PreparedStatement ps, Fascicolo fascicolo) throws SQLException {
        String idFascicolo = getIdFascicolo(dbConn, ps, fascicolo);

        String sqlText = 
                "DELETE FROM " + getFascicoliGdDocTable() +
                "WHERE id_fascicolo = ? AND id_gddoc = (" +
                    "SELECT id_gddoc " +
                    "FROM " + getGdDocTable() + " " +
                    "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?" +
                ")";
        ps = dbConn.prepareStatement(sqlText);
        int index = 1;
        ps.setString(index++, idFascicolo);
        ps.setString(index++, gdDoc.getIdOggettoOrigine());
        ps.setString(index++, gdDoc.getTipoOggettoOrigine());

        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        int result = ps.executeUpdate();
        if (result <= 0)
            throw new SQLException("fascicolo " + fascicolo.getCodiceFascicolo() + " non trovato");
    }
    
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

    public String insertGdDoc(Connection dbConn, PreparedStatement ps) throws SQLException, IOException, ServletException, UnsupportedEncodingException, MimeTypeException, IodaDocumentException, IodaFileException {
        
        String sqlText =
                        "INSERT INTO " + getGdDocTable() + "(" +
                        "id_gddoc, nome_gddoc, tipo_gddoc, " +
                        "data_ultima_modifica, stato_gd_doc, " +
                        "data_gddoc, guid_gddoc, codice_registro, " +
                        "data_registrazione, numero_registrazione, " +
                        "anno_registrazione, xml_specifico_parer, " +
                        "forza_conservazione, forza_accettazione, forza_collegamento, " +
                        "id_oggetto_origine, tipo_oggetto_origine) " +
                        "VALUES (" +
                        "?, ?, ?, " +
                        "?, ?, " +
                        "?, ?, ?, " +
                        "?, ?, " +
                        "?, ?, " +
                        "?, ?, ?, " +
                        "?, ?)";
        ps = dbConn.prepareStatement(sqlText);
        int index = 1;

        String idGdDoc = (String) gdDocIndeId.get(IodaDocumentUtilities.INDE_DOCUMENT_ID_PARAM_NAME);
        String guidGdDoc = (String) gdDocIndeId.get(IodaDocumentUtilities.INDE_DOCUMENT_GUID_PARAM_NAME);

        // id_gddoc
        ps.setString(index++, idGdDoc);

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
        ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        
        // guid_gddoc
        ps.setString(index++, guidGdDoc);
        
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

        // xml_specifico_parer
        ps.setString(index++, gdDoc.getXmlSpecificoParer());

        // forza_conservazione
        ps.setInt(index++, gdDoc.isForzaConservazione() != null && gdDoc.isForzaConservazione() ? -1 : 0);
        
        // forza_accettazione
        ps.setInt(index++, gdDoc.isForzaAccettazione() != null && gdDoc.isForzaAccettazione() ? -1 : 0);
        
        // forza_collegamento
        ps.setInt(index++, gdDoc.isForzaCollegamento() != null && gdDoc.isForzaCollegamento() ? -1 : 0);

        // id_oggetto_origine
        ps.setString(index++, gdDoc.getIdOggettoOrigine());
        
        // tipo_oggetto_origine
        ps.setString(index++, gdDoc.getTipoOggettoOrigine());

        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        int result = ps.executeUpdate();
        if (result <= 0)
            throw new SQLException("Documento non inserito");

        // inseririmento fascicoli
        List<Fascicolo> fascicoli = gdDoc.getFascicoli();
        if (fascicoli != null && !fascicoli.isEmpty()) {
            for (Fascicolo fascicolo : fascicoli) {
                insertInFascicolo(dbConn, ps, fascicolo);
            }
        }
        
        // inserimento SottoDocumenti
        List<SottoDocumento> sottoDocumenti = gdDoc.getSottoDocumenti();
        if (sottoDocumenti != null && !sottoDocumenti.isEmpty()) {
            for (SottoDocumento sottoDocumento : sottoDocumenti) {
                insertSottoDocumento(dbConn, ps, sottoDocumento);
            }
        }
        return idGdDoc;
    }
    
    public void updateGdDoc(Connection dbConn, PreparedStatement ps) throws SQLException, IOException, ServletException, UnsupportedEncodingException, MimeTypeException, IodaDocumentException, IodaFileException {
        
        String sqlText = "UPDATE " + getGdDocTable() + "SET ";
        if (gdDoc.getNome() != null)
            sqlText += "nome_gddoc = coalesce(?, nome_gddoc), ";
        if (gdDoc.isRecord() != null)
            sqlText += "tipo_gddoc = coalesce(?, tipo_gddoc), ";
        if (gdDoc.getDataUltimaModifica() != null)
            sqlText += "data_ultima_modifica = coalesce(?, data_ultima_modifica), ";
        if (gdDoc.isVisibile() != null)
            sqlText += "stato_gd_doc = coalesce(?, stato_gd_doc), ";
        if (gdDoc.getCodiceRegistro() != null)
            sqlText += "codice_registro = coalesce(?, codice_registro), ";
        if (gdDoc.getDataRegistrazione() != null)
            sqlText += "data_registrazione = coalesce(?, data_registrazione), ";
        if (gdDoc.getNumeroRegistrazione() != null)
            sqlText += "numero_registrazione = coalesce(?, numero_registrazione), ";
        if (gdDoc.getAnnoRegistrazione() != null)
            sqlText += "anno_registrazione = coalesce(?, anno_registrazione), ";
        if (gdDoc.getXmlSpecificoParer() != null)
            sqlText += "xml_specifico_parer = coalesce(?, xml_specifico_parer), ";
        if (gdDoc.isForzaConservazione() != null)
            sqlText += "forza_conservazione = coalesce(?, forza_conservazione), ";
        if (gdDoc.isForzaAccettazione() != null)
            sqlText += "forza_accettazione = coalesce(?, forza_accettazione), ";
        if (gdDoc.isForzaCollegamento() != null)
            sqlText += "forza_collegamento = coalesce(?, forza_collegamento) ";

        sqlText += "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        
        
        ps = dbConn.prepareStatement(sqlText);
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

        // xml_specifico_parer
        ps.setString(index++, gdDoc.getXmlSpecificoParer());

        // forza_conservazione
        if (gdDoc.isForzaConservazione() != null)
            ps.setInt(index++, gdDoc.isForzaConservazione() ? -1 : 0);
        else
            ps.setNull(index++, Types.INTEGER);
        
        // forza_accettazione
        if (gdDoc.isForzaAccettazione() != null)
            ps.setInt(index++, gdDoc.isForzaAccettazione() ? -1 : 0);
        else
            ps.setNull(index++, Types.INTEGER);
        
        // forza_collegamento
        if (gdDoc.isForzaCollegamento() != null)
            ps.setInt(index++, gdDoc.isForzaCollegamento() ? -1 : 0);
        else
            ps.setNull(index++, Types.INTEGER);
        
        // id_oggetto_origine
        ps.setString(index++, gdDoc.getIdOggettoOrigine());
        
        // tipo_oggetto_origine
        ps.setString(index++, gdDoc.getTipoOggettoOrigine());

        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        int result = ps.executeUpdate();
        if (result <= 0)
            throw new SQLException("Documento non trovato");

        // fascicoli
        List<Fascicolo> fascicoli = gdDoc.getFascicoli();
        if (fascicoli != null && !fascicoli.isEmpty()) {
            for (Fascicolo fascicolo : fascicoli) {
                if (fascicolo.getTipoOperazione() == Document.DocumentOperationType.INSERT)
                    insertInFascicolo(dbConn, ps, fascicolo);
                else if (fascicolo.getTipoOperazione() == Document.DocumentOperationType.DELETE)
                    deleteFromFascicolo(dbConn, ps, fascicolo);
                else
                    throw new IodaDocumentException("operazione non consentita");
            }
        }
        
        // inserimento SottoDocumenti
        List<SottoDocumento> sottoDocumenti = gdDoc.getSottoDocumenti();
        if (sottoDocumenti != null && !sottoDocumenti.isEmpty()) {
            for (SottoDocumento sottoDocumento : sottoDocumenti) {
                if (sottoDocumento.getTipoOperazione() == Document.DocumentOperationType.INSERT)
                    insertSottoDocumento(dbConn, ps, sottoDocumento);
                else if (sottoDocumento.getTipoOperazione() == Document.DocumentOperationType.DELETE)
                    deleteSottoDocumento(dbConn, ps, sottoDocumento);
                else
                    throw new IodaDocumentException("operazione non consentita");
            }
        }
    }
    
    public void deleteGdDoc(Connection dbConn, PreparedStatement ps) throws SQLException, IodaDocumentException{
        String sqlText = 
                "SELECT numero_registrazione " +
                "FROM " + getGdDocTable() + " " +
                "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, gdDoc.getIdOggettoOrigine());
        ps.setString(2, gdDoc.getTipoOggettoOrigine());
        ResultSet res = ps.executeQuery();
        if (!res.next())
            throw new SQLException("documento non trovato");
        String numeroRegistrazione = res.getString(1);
        if (numeroRegistrazione != null && !numeroRegistrazione.equals(""))
            throw new IodaDocumentException("impossibile eliminare un document registrato");
        
        sqlText = 
              "SELECT s.uuid_mongo_originale, s.uuid_mongo_pdf, s.uuid_mongo_firmato " +
              "FROM " + getSottoDocumentiTable() + " s INNER JOIN " + getGdDocTable() + " g ON s.id_gddoc = g.id_gddoc " +
              "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, gdDoc.getIdOggettoOrigine());
        ps.setString(2, gdDoc.getTipoOggettoOrigine());
        res = ps.executeQuery();
        while (res.next()) {
            String uuid = res.getString(1);
            if (uuid != null && !uuid.equals(""))
                uploadedUuids.add(uuid);

            uuid = res.getString(2);
            if (uuid != null && !uuid.equals(""))
                uploadedUuids.add(uuid);

            uuid = res.getString(3);
            if (uuid != null && !uuid.equals(""))
                uploadedUuids.add(uuid);
        }
        
        sqlText =
                "DELETE " +
                "FROM " + getGdDocTable() + " " +
                "WHERE id_oggetto_origine = ? AND tipo_oggetto_origine = ?";
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, gdDoc.getIdOggettoOrigine());
        ps.setString(2, gdDoc.getTipoOggettoOrigine());
        int ris = ps.executeUpdate();

        if (ris > 0)
            if (uploadedUuids != null && !uploadedUuids.isEmpty())
                deleteAllMongoFileUploaded();
    }   
    
    public void insertSottoDocumento(Connection dbConn, PreparedStatement ps, SottoDocumento sd) throws SQLException, IOException, ServletException, UnsupportedEncodingException, MimeTypeException, IodaDocumentException, IodaFileException {

        sd.setPrefissoApplicazioneOrigine(idApplicazione);
        JSONObject newIds = getNextIndeId();
        String idSottoDocumento = (String) newIds.get(INDE_DOCUMENT_ID_PARAM_NAME);
        String guidSottoDocumento = (String) newIds.get(INDE_DOCUMENT_GUID_PARAM_NAME);

        String idGdDoc = (String) gdDocIndeId.get(INDE_DOCUMENT_ID_PARAM_NAME);

        String mongoPath = getMongoPath();

        String sqlText = 
                "INSERT INTO " + getSottoDocumentiTable() + "(" +
                "id_sottodocumento, guid_sottodocumento, id_gddoc, nome_sottodocumento, " +
                "uuid_mongo_pdf, dimensione_pdf, convertibile_pdf, " +
                "uuid_mongo_originale, dimensione_originale, " +
                "uuid_mongo_firmato, dimensione_firmato, " +
                "principale, tipo_sottodocumento, tipo_firma, " +
                "mimetype_file_originale, mimetype_file_firmato, " +
                "codice_sottodocumento)" +
                "VALUES (" +
                "?, ?, ?, ?, " +
                "?, ?, ?, " +
                "?, ?, " +
                "?, ?, " +
                "?, ?, ?, " +
                "?, ?, " +
                "?)";
        ps = dbConn.prepareStatement(sqlText);
        int index = 1;

        ps.setString(index++, idSottoDocumento);
        ps.setString(index++, guidSottoDocumento);
        ps.setString(index++, idGdDoc);
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

        // aggiungo in una lista i sottodocumenti potenzialmente convertibili in pdf (cioè quelli per cui, non mi è stato passato l'uuid del file in pdf), il controllo
        // per verifivcare se la conversione è supportata verrà fatto successivamente
        if (sd.getUuidMongoPdf() == null || sd.getUuidMongoPdf().equals("")) {
            toConvert.add(sd);
        }
        
        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        int result = ps.executeUpdate();
        if (result <= 0)
            throw new SQLException("SottoDocumento non inserito");
    }
    
    public void deleteSottoDocumento(Connection dbConn, PreparedStatement ps, SottoDocumento sd) throws SQLException {
        sd.setPrefissoApplicazioneOrigine(idApplicazione);

        String sqlText = 
                "SELECT uuid_mongo_originale, uuid_mongo_pdf, uuid_mongo_firmato " +
                "FROM " + getSottoDocumentiTable() + " " +
                "WHERE codice_sottodocumento = ?";
        
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, sd.getCodiceSottoDocumento());
        ResultSet res = ps.executeQuery();
        
        if (!res.next())
            throw new SQLException("SottoDocumento non trovato");
        do {
            String uuid = res.getString(1);
            if (uuid != null && !uuid.equals(""))
                uploadedUuids.add(uuid);

            uuid = res.getString(2);
            if (uuid != null && !uuid.equals(""))
                uploadedUuids.add(uuid);

            uuid = res.getString(3);
            if (uuid != null && !uuid.equals(""))
                uploadedUuids.add(uuid);
        }
        while (res.next());
        ps.close();
        
        sqlText = 
                "DELETE FROM " + getSottoDocumentiTable() +
                "WHERE codice_sottodocumento = ?";
        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, sd.getCodiceSottoDocumento());
        
        String query = ps.toString();
        log.debug("eseguo la query: " + query + " ...");
        int result = ps.executeUpdate();
        if (result <= 0)
            throw new SQLException("SottoDocumento non trovato");
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

    public void deleteAllMongoFileUploaded() {
        for (String uuid : uploadedUuids) {
            mongo.delete(uuid);
        }
    }

    private void getIndeId() throws IOException, MalformedURLException, SendHttpMessageException {
            
            // contruisco la mappa dei parametri per la servlet (va passato solo un parametro che è il numero di id da generare)
            Map<String, byte[]> params = new HashMap<String, byte[]>();
            
            // il numero di id è ottenuto dal numero dei SottoDocumenti + il GdDoc
            int idNumber = 1;
             if (gdDoc.getFascicoli() != null)
                idNumber += gdDoc.getFascicoli().size();
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
                            ps.setString(3, idApplicazione + "_" + sd.getCodiceSottoDocumento());
                            
                            log.debug("eseguo la query: " + ps.toString() + "...");
                            int res = ps.executeUpdate();
                            if (res == 0)
                                throw new SQLException("sottoDocumento: " + sd.getCodiceSottoDocumento() + " non trovato");
                    }
                }
                catch (Exception ex) {
                    log.error(ex);
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
    
     
}