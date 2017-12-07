package it.bologna.ausl.bds_tools.ioda.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.bologna.ausl.bds_tools.SetDocumentNumber;
import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import static it.bologna.ausl.bds_tools.jobs.VersatoreParer.INDE_DOCUMENT_GUID_PARAM_NAME;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.bds_tools.utils.Registro;
import it.bologna.ausl.bds_tools.utils.UtilityFunctions;
import it.bologna.ausl.ioda.iodaobjectlibrary.ClassificazioneFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicoli;
import it.bologna.ausl.ioda.iodaobjectlibrary.FascicoliPregressiMap;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.FascicoliSpecialiResearcher;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo.Creazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo.StatoFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Search;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class IodaFascicoliUtilities {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(IodaFascicolazioniUtilities.class);

    private static final String GENERATE_INDE_NUMBER_PARAM_NAME = "generateidnumber";
    private static final String INDE_DOCUMENT_ID_PARAM_NAME = "document_id";
    public static final String INDE_DOCUMENT_GUID_PARAM_NAME = "document_guid";
    public static final String CODICE_REGISTRO_FASCICOLO = "FASCICOLO";

    private String gdDocTable;
    private String fascicoliTable;
    private String titoliTable;
    private String fascicoliGdDocTable;
    private String fascicoliGdVicari;
    private String utentiTable;
    private String prefixIds; // prefisso da anteporre agli id dei documenti che si inseriscono o che si ricercano (GdDoc, SottoDocumenti)
    private Search researcher;
    private Fascicolo fascicolo;
    HttpServletRequest request;

    private IodaFascicoliUtilities(Search r) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this.gdDocTable = ApplicationParams.getGdDocsTableName();
        this.fascicoliTable = ApplicationParams.getFascicoliTableName();
        this.titoliTable = ApplicationParams.getTitoliTableName();
        this.fascicoliGdDocTable = ApplicationParams.getFascicoliGdDocsTableName();
        this.utentiTable = ApplicationParams.getUtentiTableName();
        this.fascicoliGdVicari = ApplicationParams.getFascicoliGdVicariTableName();

        this.researcher = r;
    }

    public IodaFascicoliUtilities(HttpServletRequest request, Search r) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(r);
        this.request = request;
//        this.researcher = r;
    }

    public IodaFascicoliUtilities(HttpServletRequest request, Fascicolo fascicolo) throws UnknownHostException, IOException, MalformedURLException, SendHttpMessageException, IodaDocumentException {
        this(null);
        this.request = request;
        this.fascicolo = fascicolo;
    }

    public Fascicoli getFascicoli(Connection dbConn, PreparedStatement ps) throws SQLException {
        Fascicoli fascicoli = getFascicoli(dbConn, ps, researcher.getSearchString(), researcher.getIdUtente(), researcher.getLimiteDiRicerca());

        return fascicoli;
    }

    private Fascicoli getFascicoli(Connection dbConn, PreparedStatement ps, String strToFind, String idUtente, int limit) throws SQLException {

        Fascicoli res = new Fascicoli();

        // La stringa cercata è una numerazione gerarchica? (Deve iniziare con un numero)
        boolean matcha = strToFind.matches("(\\d+-?)"
                + // n- , n
                "|(\\d+/\\d*)"
                + // n/ , n/n
                "|(\\d+\\-\\d+/?)"
                + // n-n/ , n-n
                "|(\\d+\\-\\d+/\\d+)"
                + // n-n/n
                "|(\\d+\\-\\d+\\-\\d*)"
                + // n-n- , n-n-n
                "|(\\d+\\-\\d+\\-\\d+/\\d*)");   // n-n-n/n , n-n-n/

        String whereCondition;

        if (matcha) {
            whereCondition = "where f.numerazione_gerarchica ilike ('" + strToFind + "%')"; // In questo caso si vuole l'"inizia con"
        } else {
            whereCondition = "where f.nome_fascicolo ilike ('%" + strToFind + "%')";
        }

        // Preparo la query
        String sqlText = "select distinct(f.id_fascicolo), f.id_livello_fascicolo, "
                + "CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) "
                + "WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) "
                + "ELSE nome_fascicolo "
                + "END as nome_fascicolo_interfaccia, "
                + "f.numero_fascicolo, "
                + "f.nome_fascicolo,f.anno_fascicolo, f.id_utente_creazione, f.data_creazione, "
                + "f.numerazione_gerarchica, f.id_utente_responsabile, uResp.nome, uResp.cognome, "
                + "f.stato_fascicolo, f.id_utente_responsabile_proposto, f.id_fascicolo_padre, "
                + "f.id_titolo, f.speciale, t.codice_gerarchico || '' || t.codice_titolo || ' ' || t.titolo as titolo, "
                + "f.id_fascicolo_importato, f.data_chiusura, f.note_importazione, "
                + "case when fv.id_fascicolo is null then 0 else -1 end as accesso "
                + "from "
                + "gd.fascicoligd f "
                + "left join procton.titoli t on t.id_titolo = f.id_titolo "
                + "left join gd.log_azioni_fascicolo laf on laf.id_fascicolo= f.id_fascicolo "
                + "join gd.fascicoli_modificabili fv on fv.id_fascicolo=f.id_fascicolo and fv.id_utente= ? "
                + "join procton.utenti uResp on uResp.id_utente=f.id_utente_responsabile "
                + "join procton.utenti uCrea on f.id_utente_creazione=uCrea.id_utente "
                + whereCondition + " "
                + "and f.numero_fascicolo != '0' and f.speciale != -1 and f.stato_fascicolo != 'p' "
                + "and f.stato_fascicolo != 'c' order by f.nome_fascicolo";

        // controllo se ci sono limiti da applicare
        if (limit != 0) {
            sqlText += " limit " + limit;
        }

        ps = dbConn.prepareStatement(sqlText);

        ps.setString(1, idUtente);
        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();

        while (results.next()) {
            int index = 1;
            String idFascicolo = results.getString(index++);
            String idLivelloFascicolo = results.getString(index++);
            String nomeFascicoloInterfaccia = results.getString(index++);
            int numeroFascicolo = results.getInt(index++);
            String nomeFascicolo = results.getString(index++);
            int annoFascicolo = results.getInt(index++);
            String idUtenteCreazione = results.getString(index++);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dataCreazione = DateTime.parse(results.getString(index++), formatter);
            String numerazioneGerarchica = results.getString(index++);
            String idUtenteResponsabile = results.getString(index++);
            String nomeUtenteResponsabile = results.getString(index++);
            String cognomeUtenteResponsabile = results.getString(index++);
            String statoFascicolo = results.getString(index++);
            String idUtenteResponsabileProposto = results.getString(index++);
            String idFascicoloPadre = results.getString(index++);
            String idTitolo = results.getString(index++);
            int speciale = results.getInt(index++);
            String titolo = results.getString(index++);
            String idFascicoloImportato = results.getString(index++);
            DateTime dataChiusura = null;
            String tmp = results.getString(index++);
            if (tmp != null) {
                dataChiusura = DateTime.parse(tmp, formatter);
            }
            String noteImportazione = results.getString(index++);
            int accesso = results.getInt(index++);

            String descrizioneUtenteResponsabile = nomeUtenteResponsabile + " " + cognomeUtenteResponsabile;
            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setNomeFascicoloInterfaccia(nomeFascicoloInterfaccia);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDescrizioneUtenteResponsabile(descrizioneUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setSpeciale(speciale);
            f.setTitolo(titolo);
            f.setAccesso(accesso);
            f.setIdUtenteResponsabileProposto(idUtenteResponsabileProposto);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);

            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));

            res.addFascicolo(f);
        }

        if (res.getSize() == 0) {
            res.createFascicoli();
        }

        return res;
    }

    public FascicoliPregressiMap getFascicoliPregressi(Connection dbConn, PreparedStatement ps) throws SQLException, JsonProcessingException {
        return getFascicoliPregressi(dbConn, ps, researcher.getSearchString(), researcher.getIdUtente());
    }

    private FascicoliPregressiMap getFascicoliPregressi(Connection dbConn, PreparedStatement ps, String str, String idUtente) throws SQLException {

        JSONArray jsonArray = (JSONArray) JSONValue.parse(str);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            list.add((String) jsonArray.get(i));
        }
        String idList = "'" + StringUtils.join(list, "','") + "'";

        FascicoliPregressiMap res = new FascicoliPregressiMap();

        String sqlText = "SELECT distinct(f.id_fascicolo), f.id_livello_fascicolo, "
                + "   CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) "
                + "		WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) "
                + "		ELSE nome_fascicolo "
                + "        END as nome_fascicolo_interfaccia, "
                + "        f.numero_fascicolo, "
                + "        f.nome_fascicolo,f.anno_fascicolo, f.id_utente_creazione, f.data_creazione, "
                + "        f.numerazione_gerarchica, f.id_utente_responsabile, uResp.nome, uResp.cognome, "
                + "        f.stato_fascicolo, f.id_utente_responsabile_proposto, f.id_fascicolo_padre, "
                + "        f.id_titolo, f.speciale, t.codice_gerarchico || '' || t.codice_titolo || ' ' || t.titolo as titolo, "
                + "        f.id_fascicolo_importato, f.data_chiusura, f.note_importazione, f.guid_fascicolo "
                + "FROM gd.fascicoligd f "
                + "	LEFT join procton.titoli t ON t.id_titolo = f.id_titolo "
                + "	JOIN procton.utenti uResp ON uResp.id_utente=f.id_utente_responsabile "
                + "	JOIN procton.utenti uCrea ON f.id_utente_creazione=uCrea.id_utente "
                + "WHERE 1=1  "
                + "	AND f.guid_fascicolo in (" + idList + ") ";

        ps = dbConn.prepareStatement(sqlText);

        log.debug("Query: " + ps);

        ResultSet results = ps.executeQuery();

        while (results.next()) {
            int index = 1;
            String idFascicolo = results.getString(index++);
            String idLivelloFascicolo = results.getString(index++);
            String nomeFascicoloInterfaccia = results.getString(index++);
            int numeroFascicolo = results.getInt(index++);
            String nomeFascicolo = results.getString(index++);
            int annoFascicolo = results.getInt(index++);
            String idUtenteCreazione = results.getString(index++);

            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTimeFormatter formatterWithHHmmss = DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss");

            DateTime dataCreazione = DateTime.parse(results.getString(index++), formatter);
            String numerazioneGerarchica = results.getString(index++);
            String idUtenteResponsabile = results.getString(index++);
            String nomeUtenteResponsabile = results.getString(index++);
            String cognomeUtenteResponsabile = results.getString(index++);
            String statoFascicolo = results.getString(index++);
            String idUtenteResponsabileProposto = results.getString(index++);
            String idFascicoloPadre = results.getString(index++);
            String idTitolo = results.getString(index++);
            int speciale = results.getInt(index++);
            String titolo = results.getString(index++);
            String idFascicoloImportato = results.getString(index++);
            DateTime dataChiusura = null;
            Timestamp tmp = results.getTimestamp(index++);
            log.debug("Data chiusura fasciclo: " + tmp);
            if (tmp != null) {
                dataChiusura = new DateTime(tmp.getTime());
                log.debug("Data chiusura convertito: " + dataChiusura);
            }
            String noteImportazione = results.getString(index++);
            String guidFascicolo = results.getString(index++);

            String descrizioneUtenteResponsabile = nomeUtenteResponsabile + " " + cognomeUtenteResponsabile;
            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setNomeFascicoloInterfaccia(nomeFascicoloInterfaccia);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDescrizioneUtenteResponsabile(descrizioneUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setSpeciale(speciale);
            f.setTitolo(titolo);
            f.setIdUtenteResponsabileProposto(idUtenteResponsabileProposto);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);
            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));

            res.addFascicolo(guidFascicolo, f);
        }

        return res;
    }

    public Fascicoli getFascicoloSpeciale(Connection dbConn, PreparedStatement ps) throws SQLException {

        String sqlText = "SELECT id_fascicolo, numerazione_gerarchica, "
                + "numero_fascicolo, nome_fascicolo, anno_fascicolo, stato_fascicolo, "
                + "id_utente_creazione, id_utente_responsabile, data_creazione, id_livello_fascicolo, "
                + "id_fascicolo_importato, data_chiusura, note_importazione"
                + " FROM " + fascicoliTable
                + " WHERE anno_fascicolo = ? AND speciale = ?";
        ps = dbConn.prepareStatement(sqlText);

        FascicoliSpecialiResearcher specRes = (FascicoliSpecialiResearcher) researcher;
//        ps.setString(1, specRes.getNomeFascicolo());
        ps.setInt(1, specRes.getAnno());
        ps.setInt(2, -1);
        log.debug("sql: " + ps.toString());

        ResultSet results = ps.executeQuery();

        Fascicoli fascicoliSpeciali = new Fascicoli();

        while (results.next()) {

            String idFascicolo = results.getString(1);
            String numerazioneGerarchica = results.getString(2);
            int numeroFascicolo = results.getInt(3);
            String nomeFascicolo = results.getString(4);
            int annoFascicolo = results.getInt(5);
            String statoFascicolo = results.getString(6);
            String idUtenteCreazione = results.getString(7);
            String idUtenteResponsabile = results.getString(8);
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTime dataCreazione = DateTime.parse(results.getString(9), formatter);
            String idLivelloFascicolo = results.getString(10);
            String idFascicoloImportato = results.getString(11);
            DateTime dataChiusura = null;
            if (results.getString(12) != null) {
                dataChiusura = DateTime.parse(results.getString(12), formatter);
            }
            String noteImportazione = results.getString(13);

            Fascicolo f = new Fascicolo();
            f.setCodiceFascicolo(numerazioneGerarchica);
            f.setNumeroFascicolo(numeroFascicolo);
            f.setNomeFascicolo(nomeFascicolo);
            f.setIdUtenteResponsabile(idUtenteResponsabile);
            f.setDataCreazione(dataCreazione);
            f.setAnnoFascicolo(annoFascicolo);
            f.setIdUtenteCreazione(idUtenteCreazione);
            f.setStatoFascicolo(statoFascicolo);
            f.setIdLivelloFascicolo(idLivelloFascicolo);
            f.setIdFascicoloImportato(idFascicoloImportato);
            f.setDataChiusura(dataChiusura);
            f.setNoteImportazione(noteImportazione);

            f.setClassificazioneFascicolo(getClassificazioneFascicolo(dbConn, ps, idFascicolo));

            fascicoliSpeciali.addFascicolo(f);
        }

        return fascicoliSpeciali;
    }

    private ClassificazioneFascicolo getClassificazioneFascicolo(Connection dbConn, PreparedStatement ps, String idFascicolo) throws SQLException {

        ClassificazioneFascicolo classificazioneFascicolo = null;

        String sqlText
                = "SELECT t.codice_gerarchico, t.codice_titolo "
                + "FROM " + getFascicoliTable() + " f, " + getTitoliTable() + " t "
                + "WHERE id_fascicolo = ? "
                + "AND f.id_titolo = t.id_titolo ";

        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idFascicolo);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();

        String result;

        if (!res.next()) {
            log.error("SQLException: titolo non trovato");
            throw new SQLException("titolo non trovato");
        } else {
            result = res.getString(1) + res.getString(2);

            classificazioneFascicolo = new ClassificazioneFascicolo();

            // calcolo la gerarchia e setta il corrispettivo nome
            String[] parts = result.split("-");
            int dim = parts.length;

            while (dim > 0) {
                switch (dim) {
                    case 3:
                        String nomeSottoClasse = this.getTitolo(dbConn, ps, parts[0] + "-" + parts[1] + "-", parts[2]);
                        classificazioneFascicolo.setNomeSottoclasse(nomeSottoClasse);
                        classificazioneFascicolo.setCodiceSottoclasse(parts[0] + "-" + parts[1] + "-" + parts[2]);
                        break;

                    case 2:
                        String nomeClasse = this.getTitolo(dbConn, ps, parts[0] + "-", parts[1]);
                        classificazioneFascicolo.setNomeClasse(nomeClasse);
                        classificazioneFascicolo.setCodiceClasse(parts[0] + "-" + parts[1]);
                        break;

                    case 1:
                        String nomeCategoria = this.getTitolo(dbConn, ps, null, parts[0]);
                        classificazioneFascicolo.setNomeCategoria(nomeCategoria);
                        classificazioneFascicolo.setCodiceCategoria(parts[0]);
                        break;
                }
                dim--;
            }
        }
        return classificazioneFascicolo;
    }

    private String getTitolo(Connection dbConn, PreparedStatement ps, String codiceGerarchico, String codiceTitolo) throws SQLException {

        String sqlText = "SELECT titolo "
                + "FROM " + getTitoliTable() + " "
                + "WHERE codice_gerarchico = ? "
                + "AND codice_titolo = ? ";

        ps = dbConn.prepareStatement(sqlText);

        if (codiceGerarchico != null && !"".equals(codiceGerarchico)) {
            ps.setString(1, codiceGerarchico);
        } else {
            ps.setString(1, "");
        }

        ps.setString(2, codiceTitolo);

        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();

        if (!res.next()) {
            throw new SQLException("nessun titolo trovato");
        }
        return res.getString(1);
    }

    public String getNomeCognome(Connection dbConn, PreparedStatement ps, String idUtente) throws SQLException {

        String result = null;

        String sqlText
                = "SELECT nome, cognome "
                + "FROM " + getUtentiTable() + " "
                + "WHERE id_utente = ? ";

        ps = dbConn.prepareStatement(sqlText);
        ps.setString(1, idUtente);
        log.debug("eseguo la query: " + ps.toString() + " ...");
        ResultSet res = ps.executeQuery();

        if (!res.next()) {
            throw new SQLException("utente non trovato");
        } else {
            result = res.getString(1) + " " + res.getString(2);
        }

        return result;
    }

    public String getGdDocTable() {
        return gdDocTable;
    }

    public String getTitoliTable() {
        return titoliTable;
    }

    public String getFascicoliTable() {
        return fascicoliTable;
    }

    public String getFascicoliGdVicariTable() {
        return fascicoliGdVicari;
    }

    public String getFascicoliGdDocTable() {
        return fascicoliGdDocTable;
    }

    public String getUtentiTable() {
        return utentiTable;
    }

    public Fascicolo getFascicolo() {
        return fascicolo;
    }

    public String insertFascicolo(Connection dbConn) throws IOException, MalformedURLException, SendHttpMessageException, SQLException, ServletException {

//        ottengo in ID di INDE
        String idInde = getIndeIdAndGuid().get(INDE_DOCUMENT_ID_PARAM_NAME);
        String guidInde = getIndeIdAndGuid().get(INDE_DOCUMENT_GUID_PARAM_NAME);

        String sqlText
                = "INSERT INTO " + getFascicoliTable() + "("
                + "id_fascicolo, nome_fascicolo, numero_fascicolo, anno_fascicolo, "
                + "stato_fascicolo, id_livello_fascicolo, id_struttura, "
                + "id_titolo, id_utente_responsabile, id_utente_creazione, "
                + "id_utente_responsabile_proposto, data_creazione, data_responsabilita, "
                + "eredita_permessi, "
                + "speciale, guid_fascicolo, "
                + "id_tipo_fascicolo, codice_fascicolo, "
                + "servizio_creazione, descrizione_iter) "
                + "VALUES (?, ?, ?, ?, "
                + "?, ?, ?, "
                + "?, ?, ?, "
                + "?, ?, ?, "
                + "?, "
                + "?, ?, "
                + "?, ?, "
                + "?, ?)";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;

            int year = DateTime.now().getYear();

            ps.setString(index++, idInde);
            ps.setString(index++, fascicolo.getNomeFascicolo());
            // metto il numero 0 come quando creo un fascicolo in bozza
            ps.setInt(index++, 0);
            ps.setInt(index++, year);
            // metto lo stato in bozza
            ps.setString(index++, StatoFascicolo.APERTO.getName());

            // imposto la creazione del fascicolo, per la creazione dei sottofascicoli
            // e inserti la si ripenserà se ci sarà bisogno
            ps.setString(index++, String.valueOf(1));

            ps.setString(index++, fascicolo.getIdStruttura());
            ps.setString(index++, fascicolo.getTitolo());
            ps.setString(index++, fascicolo.getIdUtenteResponsabile());
            ps.setString(index++, fascicolo.getIdUtenteCreazione());
            if (fascicolo.getIdUtenteResponsabileProposto() != null && !fascicolo.getIdUtenteResponsabileProposto().equals("")) {
                ps.setString(index++, fascicolo.getIdUtenteResponsabileProposto());
            }

            // data creazione
            ps.setDate(index++, java.sql.Date.valueOf(java.time.LocalDate.now()));
            // data responsabilità
            ps.setDate(index++, java.sql.Date.valueOf(java.time.LocalDate.now()));
            // eredita permessi: messo a -1 perchè ho visto che i più recenti sono stati imposttai a -1 e non a 1
            ps.setInt(index++, -1);

            ps.setInt(index++, fascicolo.getSpeciale());
            ps.setString(index++, guidInde);
            // idTipoFascicolo
            ps.setInt(index++, fascicolo.getIdTipoFascicolo());
            // codiceFascicolo
            ps.setString(index++, "babel_" + guidInde);
            // setto il servizio di creazione con valore API
            ps.setString(index++, Creazione.API.getName());
            ps.setString(index++, fascicolo.getDescrizioneIter());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            int result = ps.executeUpdate();
            if (result <= 0) {
                throw new SQLException("Fascicolo non inserito");
            }

            insertVicari(dbConn, idInde);

            registraDocumento(dbConn, ps, guidInde, CODICE_REGISTRO_FASCICOLO);

            return guidInde;
        }
    }

    public void insertVicari(Connection dbConn, String idFascicolo) throws SQLException {

        String sqlText
                = "INSERT INTO " + getFascicoliGdVicariTable() + "("
                + "id_fascicolo, id_utente) "
                + "VALUES (?, ?)";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {

            for (String vicario : fascicolo.getVicari()) {
                ps.setString(1, idFascicolo);
                ps.setString(2, vicario);

                String query = ps.toString();
                log.debug("eseguo la query: " + query);
                int result = ps.executeUpdate();
                if (result <= 0) {
                    throw new SQLException("Fascicolo non inserito");
                }
            }
        }
    }

    /**
     * Restituisce una mappa contene id di INDE
     *
     * @return
     * @throws IOException
     * @throws MalformedURLException
     * @throws SendHttpMessageException
     */
    private Map<String, String> getIndeIdAndGuid() throws IOException, MalformedURLException, SendHttpMessageException {

        Map<String, String> result = new HashMap<>();

        // contruisce la mappa dei parametri per la servlet (va passato solo un parametro che è il numero di id da generare)
        Map<String, byte[]> params = new HashMap<>();

        // ottene un docid
        int idNumber = 1;
        params.put(GENERATE_INDE_NUMBER_PARAM_NAME, String.valueOf(idNumber).getBytes());

        String res = UtilityFunctions.sendHttpMessage(ApplicationParams.getGetIndeUrlServiceUri(), null, null, params, "POST", null);

        // la servlet torna un JsonObject che è una coppia (id, guid)
        JSONArray indeIdArray = (JSONArray) JSONValue.parse(res);
        JSONObject currentId = (JSONObject) indeIdArray.get(0);
        result.put(INDE_DOCUMENT_ID_PARAM_NAME, (String) currentId.get(INDE_DOCUMENT_ID_PARAM_NAME));
        result.put(INDE_DOCUMENT_GUID_PARAM_NAME, (String) currentId.get(INDE_DOCUMENT_GUID_PARAM_NAME));
        return result;
    }

    public String registraDocumento(Connection dbConn, PreparedStatement ps, String guid, String codiceRegistro) throws ServletException, SQLException {
        Registro r = Registro.getRegistro(codiceRegistro, dbConn);
        return SetDocumentNumber.setNumber(dbConn, guid, r.getSequenzaAssociata());
    }

    public Fascicolo getFascicolo(Connection dbConn, String guid) throws SQLException {

        Fascicolo fascicolo = new Fascicolo();
        String idFascicolo = null;

        String sqlText
                = "SELECT id_fascicolo, nome_fascicolo, numero_fascicolo, anno_fascicolo, "
                + "stato_fascicolo, id_livello_fascicolo, id_struttura, "
                + "id_titolo, id_utente_responsabile, id_utente_creazione, "
                + "id_utente_responsabile_proposto, data_creazione, "
                + "numerazione_gerarchica, "
                + "speciale, "
                + "id_tipo_fascicolo, codice_fascicolo, data_registrazione, "
                + "servizio_creazione, descrizione_iter "
                + "FROM " + getFascicoliTable() + " "
                + "WHERE guid_fascicolo = ? ";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, guid);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();

            while (result.next()) {
                idFascicolo = result.getString("id_fascicolo");
                fascicolo.setNomeFascicolo(result.getString("nome_fascicolo"));
                fascicolo.setNumeroFascicolo(result.getInt("numero_fascicolo"));
                fascicolo.setAnnoFascicolo(result.getInt("anno_fascicolo"));
                fascicolo.setStatoFascicolo(result.getString("stato_fascicolo"));
                fascicolo.setIdLivelloFascicolo(result.getString("id_livello_fascicolo"));
                fascicolo.setIdStruttura(result.getString("id_struttura"));
                fascicolo.setTitolo(result.getString("id_titolo"));
                fascicolo.setIdUtenteResponsabile(result.getString("id_utente_responsabile"));
                fascicolo.setIdUtenteCreazione(result.getString("id_utente_creazione"));
                fascicolo.setIdUtenteResponsabileProposto(result.getString("id_utente_responsabile_proposto"));

                Timestamp dataCreazione = result.getTimestamp("data_creazione");
                if (dataCreazione != null) {
                    fascicolo.setDataCreazione(new DateTime(dataCreazione.getTime()));
                }

                fascicolo.setNumerazioneGerarchica(result.getString("numerazione_gerarchica"));
                fascicolo.setSpeciale(result.getInt("speciale"));
                fascicolo.setIdTipoFascicolo(result.getInt("id_tipo_fascicolo"));
                fascicolo.setCodiceFascicolo(result.getString("codice_fascicolo"));
                Timestamp dataRegistrazione = result.getTimestamp("data_registrazione");
                if (dataRegistrazione != null) {
                    fascicolo.setDataRegistrazione(new DateTime(dataRegistrazione.getTime()));
                }

                fascicolo.setServizioCreazione(result.getString("servizio_creazione"));
                fascicolo.setDescrizioneIter(result.getString("descrizione_iter"));
            }
        }

        String sqlTextVicari
                = "SELECT id_utente "
                + "FROM " + getFascicoliGdVicariTable() + " "
                + "WHERE id_fascicolo = ? ";

        try (PreparedStatement ps = dbConn.prepareStatement(sqlTextVicari)) {
            ps.setString(1, idFascicolo);

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet result = ps.executeQuery();

            while (result.next()) {
                fascicolo.addVicario(result.getString("id_utente"));
            }
        }
        return fascicolo;
    }
}
