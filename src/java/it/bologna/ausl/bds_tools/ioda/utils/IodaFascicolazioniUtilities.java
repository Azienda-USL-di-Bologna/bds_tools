package it.bologna.ausl.bds_tools.ioda.utils;

import it.bologna.ausl.bds_tools.exceptions.SendHttpMessageException;
import it.bologna.ausl.bds_tools.utils.ApplicationParams;
import it.bologna.ausl.ioda.iodaobjectlibrary.BagProfiloArchivistico;
import it.bologna.ausl.ioda.iodaobjectlibrary.ClassificazioneFascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazione;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolazioni;
import it.bologna.ausl.ioda.iodaobjectlibrary.Fascicolo;
import it.bologna.ausl.ioda.iodaobjectlibrary.SimpleDocument;
import it.bologna.ausl.ioda.iodaobjectlibrary.exceptions.IodaDocumentException;
import it.bologna.ausl.riversamento.builder.ProfiloArchivistico;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class IodaFascicolazioniUtilities {
    
    private static final Logger log = LogManager.getLogger(IodaFascicolazioniUtilities.class);
    
    private String gdDocTable;
    private String fascicoliTable;
    private String titoliTable;
    private String titoliVersioniTable;
    private String titoliVersioniCrossTable;
    private String fascicoliGdDocTable;
    private String utentiTable;
    private String prefixIds; // prefisso da anteporre agli id dei documenti che si inseriscono o che si ricercano (GdDoc, SottoDocumenti)
    private SimpleDocument sd;
    HttpServletRequest request;
    
    public IodaFascicolazioniUtilities(SimpleDocument sd, String prefixIds) {
        this.gdDocTable = ApplicationParams.getGdDocsTableName();
        this.fascicoliTable = ApplicationParams.getFascicoliTableName();
        this.titoliTable = ApplicationParams.getTitoliTableName();
        this.titoliVersioniTable = ApplicationParams.getTitoliVersioniTableName();
        this.titoliVersioniCrossTable = ApplicationParams.getTitoliVersioniCrossTableName();
        this.fascicoliGdDocTable = ApplicationParams.getFascicoliGdDocsTableName();
        this.utentiTable = ApplicationParams.getUtentiTableName();
        this.prefixIds = prefixIds;
        this.sd = sd;
        //sd.setPrefissoApplicazioneOrigine(this.prefixIds);
        
    }

    @Deprecated
    public Fascicolazioni getFascicolazioniOld(Connection dbConn) throws SQLException{
        
        Fascicolazioni fascicolazioni = new Fascicolazioni(sd.getIdOggettoOrigine(), sd.getTipoOggettoOrigine());
        
        // estrazione dei fascicoli del documento
        ArrayList<String> idFascicoli = this.getIdFascicoli(dbConn, false);
        
        // per ogni fascicolo calcolo la sua classificazione
        for (String idFascicolo : idFascicoli) {
            ClassificazioneFascicolo classificazioneFascicolo = this.getClassificazioneFascicolo(dbConn, idFascicolo);
            Fascicolazione fascicolazione = getFascicolazione(dbConn, idFascicolo, classificazioneFascicolo);
            fascicolazioni.addFascicolazione(fascicolazione);
        }
        return fascicolazioni;
    }

    public List<Fascicolazione> getFascicolazioni(Connection dbConn, boolean escludiSpeciali) throws SQLException{
        List<Fascicolazione> fascicolazioni = new ArrayList<>();

        // estrazione dei fascicoli del documento
        ArrayList<String> idFascicoli = this.getIdFascicoli(dbConn, escludiSpeciali);

        // per ogni fascicolo calcolo la sua classificazione
        for (String idFascicolo : idFascicoli) {
            ClassificazioneFascicolo classificazioneFascicolo = this.getClassificazioneFascicolo(dbConn, idFascicolo);
            Fascicolazione fascicolazione = getFascicolazione(dbConn, idFascicolo, classificazioneFascicolo);
            fascicolazioni.add(fascicolazione);
        }
        return fascicolazioni;
    }

    private String getTitolo(Connection dbConn, String codiceGerarchico, String codiceTitolo) throws SQLException {
        
//        String sqlTextOld = "SELECT titolo " +
//                  "FROM " + getTitoliTable() + " " + 
//                  "WHERE codice_gerarchico = ? " +
//                  "AND codice_titolo = ? ";
        
//        String sqlText = "SELECT titolo " +
//                  "FROM " + getTitoliTable() + " t, " + getTitoliVersioniTable() + " vt, " + getTitoliVersioniCrossTable() + " tvc " + 
//                  "WHERE codice_gerarchico = ? " +
//                  "AND codice_titolo = ? " +
//                  "AND vt.stato = 'C' " +
//                  "AND t.id_titolo = tvc.id_titolo " +
//                  "and tvc.id_versione = vt.id_versione ";

        String sqlText = "SELECT titolo " +
                  "FROM " + getTitoliTable() + " t, " + getTitoliVersioniTable() + " vt, " + getTitoliVersioniCrossTable() + " tvc " + 
                  "WHERE codice_gerarchico = ? " +
                  "AND codice_titolo = ? " +
                  "AND t.id_titolo = tvc.id_titolo " +
                  "AND tvc.id_versione = vt.id_versione " +
                  "GROUP BY t.titolo, t.data_ora_creazione " +
                  "ORDER BY t.data_ora_creazione DESC";
        
        String res = null;
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
        
            if(codiceGerarchico != null && !"".equals(codiceGerarchico)){
                ps.setString(1, codiceGerarchico);
            }
            else{
                ps.setString(1, "");
            }

            ps.setString(2, codiceTitolo);

            log.debug("eseguo la query: " + ps.toString() + " ...");
            ResultSet queryRes = ps.executeQuery();

            if (!queryRes.next())
                throw new SQLException("nessun titolo trovato");
            res = queryRes.getString(1);
        }
        return res;
    }

    private ArrayList<String> getIdFascicoli(Connection dbConn, boolean escludiSpeciali) throws SQLException {
        
        ArrayList<String> res = new ArrayList<>(); 
        
        String sqlText = null;
        
        if (escludiSpeciali){
            sqlText =
            "SELECT fg.id_fascicolo " +
            "FROM " + getGdDocTable() + " g, " + getFascicoliGdDocTable() + " fg,  " + getFascicoliTable() + " f " +
            "WHERE g.id_oggetto_origine = ? " +
            "AND g.tipo_oggetto_origine = ? " +
            "AND g.id_gddoc = fg.id_gddoc " +
            "AND f.id_fascicolo = fg.id_fascicolo " +
            "AND f.speciale = 0";
        }
        else{
            sqlText =
            "SELECT fg.id_fascicolo " +
            "FROM " + getGdDocTable() + " g, " + getFascicoliGdDocTable() + " fg " +
            "WHERE g.id_oggetto_origine = ? " +
            "AND g.tipo_oggetto_origine = ? " +
            "AND g.id_gddoc = fg.id_gddoc ";
        }
        
        

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            int index = 1;
            ps.setString(index++, sd.getIdOggettoOrigine());
            ps.setString(index++, sd.getTipoOggettoOrigine());

            String query = ps.toString();
            log.debug("eseguo la query: " + query + " ...");
            ResultSet results = ps.executeQuery();
            while (results.next()) {
                res.add(results.getString(1));
            }
        }
        return res;
    }

    private ClassificazioneFascicolo getClassificazioneFascicolo(Connection dbConn, String idFascicolo) throws SQLException{
        
        ClassificazioneFascicolo classificazioneFascicolo = null;
        
        String sqlText = 
                "SELECT t.codice_gerarchico, t.codice_titolo " +
                "FROM " + getFascicoliTable() + " f, " + getTitoliTable() + " t " +
                "WHERE id_fascicolo = ? " +
                "AND f.id_titolo = t.id_titolo ";          
      
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, idFascicolo);
            log.debug("eseguo la query: " + ps.toString() + " ...");
            ResultSet res = ps.executeQuery();

            String result;

            if (!res.next())
                throw new SQLException("titolo non trovato");
            else{
                result = res.getString(1) + res.getString(2);

                classificazioneFascicolo = new ClassificazioneFascicolo();

                // calcolo la gerarchia e setta il corrispettivo nome
                String[] parts = result.split("-");
                int dim = parts.length;

                while(dim>0){
                    switch(dim){
                    case 3:
                        String nomeSottoClasse = this.getTitolo(dbConn, parts[0] + "-" + parts[1] + "-", parts[2]);
                        classificazioneFascicolo.setNomeSottoclasse(nomeSottoClasse);
                        classificazioneFascicolo.setCodiceSottoclasse(parts[0] + "-" + parts[1] + "-" + parts[2]);
                        break;

                    case 2:
                        String nomeClasse = this.getTitolo(dbConn, parts[0] + "-", parts[1]);
                        classificazioneFascicolo.setNomeClasse(nomeClasse);
                        classificazioneFascicolo.setCodiceClasse(parts[0] + "-" + parts[1]);
                        break;

                    case 1:
                        String nomeCategoria = this.getTitolo(dbConn, null, parts[0]);
                        classificazioneFascicolo.setNomeCategoria(nomeCategoria);
                        classificazioneFascicolo.setCodiceCategoria(parts[0]);
                        break;
                    }
                    dim--;
                }  
            }
        }
        return classificazioneFascicolo;
    }


    private Fascicolazione getFascicolazione(Connection dbConn, String idFascicolo, ClassificazioneFascicolo classificazione) throws SQLException{
        
        Fascicolazione fascicolazione = null;
        
        String sqlTextOld = 
                "SELECT f.numerazione_gerarchica, f.nome_fascicolo, fg.data_assegnazione, fg.id_utente_fascicolatore, fg.data_eliminazione, fg.id_utente_eliminazione " +
                "FROM " + getFascicoliTable() + " f, " + getFascicoliGdDocTable() + " fg, "+ getGdDocTable() + " g " +
                "WHERE g.id_oggetto_origine = ? " +
                "AND g.tipo_oggetto_origine = ? " +
                "AND g.id_gddoc = fg.id_gddoc " +
                "AND f.id_fascicolo = fg.id_fascicolo " +
                "AND fg.id_fascicolo = ? ";
	
        String sqlText2 = 
                "SELECT f.numerazione_gerarchica, f.nome_fascicolo, fg.data_assegnazione, fg.id_utente_fascicolatore, fg.data_eliminazione, " +
                "fg.id_utente_eliminazione, f.numero_fascicolo, f.anno_fascicolo, " +
                "CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) " +
                "WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) " +
                "ELSE nome_fascicolo " +
                "END as nome_fascicolo_interfaccia " +
                "FROM " + getFascicoliTable() + " f, " + getFascicoliGdDocTable() + " fg, "+ getGdDocTable() + " g " +
                "WHERE g.id_oggetto_origine = ? " +
                "AND g.tipo_oggetto_origine = ? " +
                "AND g.id_gddoc = fg.id_gddoc " +
                "AND f.id_fascicolo = fg.id_fascicolo " +
                "AND fg.id_fascicolo = ? " ;
        
        String sqlText = 
                "with permesso as ( "  + 
"select fvp.id_fascicolo, f.numerazione_gerarchica, (select nome_fascicolo  from procton_tools.get_fascicolo_root(fvp.id_fascicolo)) as root " + 
"from gd.fascicoli_visibili_permessi fvp " + 
"join gd.fascicoligd f on f.id_fascicolo = fvp.id_fascicolo " + 
"where fvp.id_utente = ?  and  fvp.id_fascicolo in (?) " + 
"union " +  
"select v.id_fascicolo, f.numerazione_gerarchica ,(select nome_fascicolo from procton_tools.get_fascicolo_root(v.id_fascicolo)) as root " + 
"from gd.fascicoli_gd_vicari v " + 
"join gd.fascicoligd f on f.id_fascicolo = v.id_fascicolo " +
"where v.id_utente = ?  and  v.id_fascicolo in (?) " + 
"union " + 
"select fr.id_fascicolo , f.numerazione_gerarchica, (select nome_fascicolo from procton_tools.get_fascicolo_root(fr.id_fascicolo)) as root " + 
"from gd.fascicoli_responsabilita fr " + 
"join gd.fascicoligd f on f.id_fascicolo = fr.id_fascicolo " +
"where fr.id_utente_responsabile = ? and fr.id_fascicolo in (?) )" + 
"SELECT f.numerazione_gerarchica, f.nome_fascicolo, fg.data_assegnazione, fg.id_utente_fascicolatore, fg.data_eliminazione, " +
"	fg.id_utente_eliminazione, f.numero_fascicolo, f.anno_fascicolo, " +
"	CASE f.id_livello_fascicolo WHEN '2' THEN (select nome_fascicolo from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo) " +
"		WHEN '3' THEN (select nome_fascicolo from gd.fascicoligd where id_fascicolo = (select id_fascicolo_padre from gd.fascicoligd where f.id_fascicolo_padre = id_fascicolo)) " +
"		ELSE nome_fascicolo " +
"		END as nome_fascicolo_interfaccia, " +
"	CASE WHEN (select count(*) from permesso  ) = 0  THEN f.numerazione_gerarchica " +
"		ELSE (select root from permesso) " +
"		END as nome_fascicolo_interfaccia_omissis, " +
"	CASE WHEN (select count(*) from permesso  ) = 0  THEN false " +
"		ELSE true " +
"		END as permesso " +
"FROM " + getFascicoliTable() + " f, " + getFascicoliGdDocTable() + " fg, "+ getGdDocTable() + " g " +
"WHERE g.id_oggetto_origine = ? " +
"AND g.tipo_oggetto_origine = ? " +
"AND g.id_gddoc = fg.id_gddoc " +
"AND f.id_fascicolo = fg.id_fascicolo " +
"AND fg.id_fascicolo = ? " ;
        
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, sd.getUtente());
            ps.setString(2, idFascicolo);
            ps.setString(3, sd.getUtente());
            ps.setString(4, idFascicolo);
            ps.setString(5, sd.getUtente());
            ps.setString(6, idFascicolo);
            ps.setString(7, sd.getIdOggettoOrigine());
            ps.setString(8, sd.getTipoOggettoOrigine());
            ps.setString(9, idFascicolo);
            log.debug("eseguo la query: " + ps.toString() + " ...");
            ResultSet res = ps.executeQuery();

            if (!res.next())
                throw new SQLException("fascicolazione non trovata");
            else{
                int index = 1;
                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
                String numerazioneGerarchica = res.getString(index++);
                
                String nomeFascicolo = res.getString(index++);
                DateTime dataAssegnazione;
                String dataStr = res.getString(index++);
                
                if (dataStr != null){
                   try{
                        dataAssegnazione = DateTime.parse(dataStr, formatter);
                    } catch(Exception e){
                        formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                        dataAssegnazione = DateTime.parse(dataStr, formatter);
                    } 
                }
                else{
                    dataAssegnazione = null;
                }
                
                

                String idUtenteFascicolatore = res.getString(index++);

                // controllo che esista la data di eliminazione
                String controlloDataEliminazione = res.getString(index++);
                DateTime dataEliminazione = null;
                if(controlloDataEliminazione != null && !controlloDataEliminazione.equals("")){
                    try{
                        dataEliminazione = DateTime.parse(controlloDataEliminazione, formatter);
                    } catch(Exception e){
                        formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                        dataEliminazione = DateTime.parse(controlloDataEliminazione, formatter);
                    }
                }
                    

                // controllo che esista idUtenteEliminatore
                String idUtenteEliminatore = res.getString(index++);
                int numeroFascicolo = res.getInt(index++);
                int annoFascicolo = res.getInt(index++);
                String nomeFascicoloInterfaccia = res.getString(index++);
                String descrizioneEliminatore = null;
                boolean eliminato = false;
                if(idUtenteEliminatore != null && !idUtenteEliminatore.equals("")){
                    eliminato = true;
                    descrizioneEliminatore = this.getNomeCognome(dbConn, idUtenteEliminatore);
                }
                String descrizioneFascicolatore = null;
                if(idUtenteFascicolatore != null && !idUtenteFascicolatore.equals("")){
                    descrizioneFascicolatore = this.getNomeCognome(dbConn, idUtenteFascicolatore);
                }
                
                String nomeFascicoloInterfacciaOmissis = res.getString(index++); 
                boolean permessoFascicolo = res.getBoolean(index++);

                //fascicolazione = new Fascicolazione(numerazioneGerarchica, nomeFascicolo, idUtenteFascicolatore, descrizioneFascicolatore, dataAssegnazione, eliminato, dataEliminazione, idUtenteEliminatore, descrizioneEliminatore, classificazione, nomeFascicoloInterfaccia);
                fascicolazione = new Fascicolazione(numerazioneGerarchica, nomeFascicolo, idUtenteFascicolatore, descrizioneFascicolatore, dataAssegnazione, eliminato, dataEliminazione, idUtenteEliminatore, descrizioneEliminatore, classificazione, nomeFascicoloInterfaccia, nomeFascicoloInterfacciaOmissis, permessoFascicolo);
                fascicolazione.setAnno(annoFascicolo);
                fascicolazione.setNumero(numeroFascicolo);
            }
        }
        return fascicolazione;
    }
    
    public BagProfiloArchivistico getGerarchiaFascicolo(Connection dbConn, String numerazioneGerarchica) throws SQLException{
        
        BagProfiloArchivistico res = null;
       
        String idLivelloFascicolo = null;
        String idTitolo = null;
        String idFascicolo = null;
        String  idFascicoloPadre = null;
        String nomeFascicolo = null;
        int numeroFascicolo;
        int annoFascicolo;
        
        // determinazione livello fascicolo
        String sqlLivelloFascicolo = 
                "SELECT id_fascicolo, id_fascicolo_padre, id_livello_fascicolo, id_titolo, " + 
                "nome_fascicolo, numero_fascicolo, anno_fascicolo " + 
                "FROM " + getFascicoliTable() + " f " +
                "WHERE numerazione_gerarchica = ? ";
        
        try (PreparedStatement ps = dbConn.prepareStatement(sqlLivelloFascicolo)) {
            ps.setString(1, numerazioneGerarchica);
            log.debug("eseguo la query: " + ps.toString() + " ...");
            ResultSet r = ps.executeQuery();

            if (!r.next())
                throw new SQLException("fascicolazione non trovata");
            else{
                int index = 1;
                idFascicolo = r.getString(index++);
                idFascicoloPadre = r.getString(index++);
                idLivelloFascicolo = r.getString(index++);
                idTitolo = r.getString(index++);
                nomeFascicolo = r.getString(index++);
                numeroFascicolo = r.getInt(index++);
                annoFascicolo = r.getInt(index++);
            }
        }
        
        if (idLivelloFascicolo != null){
            log.debug("idLivelloFascicolo != null");
            log.debug("valore: "+idLivelloFascicolo);
            res = new BagProfiloArchivistico();
            
            switch(idLivelloFascicolo.trim()){
                
                case "1":
                    log.debug("case 1");
                    String sqlTextL1 = 
                            "SELECT distinct codice_gerarchico || codice_titolo " +
                            "FROM " + getTitoliTable() + " t " +
                            "WHERE t.id_titolo = ? ";
                    try (PreparedStatement ps = dbConn.prepareStatement(sqlTextL1)) {
                        ps.setString(1, idTitolo);
                        log.debug("eseguo la query: " + ps.toString() + " ...");
                        ResultSet resL1 = ps.executeQuery();

                        if (!resL1.next())
                            throw new SQLException("titolo non trovato");
                        else{
                            int index = 1;
                            String classifica = resL1.getString(index++);
                            classifica = classifica.replaceAll("-", "/");
                            res.setClassificaFascicolo(classifica);
                            res.setAnnoFascicolo(annoFascicolo);
                            res.setNomeFascicolo(nomeFascicolo);
                            res.setNumeroFasciolo(numeroFascicolo);
                            res.setLevel(1);
                        }
                    }
                break;
                
                case "2":
                    log.debug("case 2");
                    String sqlTextL2 = 
                            "SELECT distinct codice_gerarchico || codice_titolo, " +
                            "f.anno_fascicolo, f.numero_fascicolo, f.nome_fascicolo " +
                            "FROM " + getTitoliTable() + " t, " + getFascicoliTable() + " f " + 
                            "WHERE f.id_fascicolo = ? " + 
                            "AND t.id_titolo = (SELECT fg.id_titolo " +
                                                    "FROM gd.fascicoligd fg " +
                                                    "WHERE fg.id_fascicolo = ?) ";
                    try (PreparedStatement ps = dbConn.prepareStatement(sqlTextL2)) {
                        ps.setString(1, idFascicoloPadre);
                        ps.setString(2, idFascicoloPadre);
                        log.debug("eseguo la query: " + ps.toString() + " ...");
                        ResultSet resL1 = ps.executeQuery();

                        if (!resL1.next())
                            throw new SQLException("titolo non trovato");
                        else{
                            int index = 1;
                            String classifica = resL1.getString(index++);
                            int anno =  resL1.getInt(index++);
                            int numero =  resL1.getInt(index++);
                            String nome = resL1.getString(index++);
                            classifica = classifica.replaceAll("-", "/");
                            res.setClassificaFascicolo(classifica);
                            res.setAnnoFascicolo(anno);
                            res.setNomeFascicolo(nome);
                            res.setNumeroFasciolo(numero);
                            res.setNomeSottoFascicolo(nomeFascicolo);
                            res.setNumeroSottoFascicolo(numeroFascicolo);
                            res.setLevel(2);
                        }
                    }
                break;
                
                case "3":
                    log.debug("case 3");
                    String sqlTextL3 = 
                            "SELECT DISTINCT codice_gerarchico || codice_titolo, " + 
                            "ff.anno_fascicolo, ff.numero_fascicolo, ff.nome_fascicolo, " + 
                            "f.numero_fascicolo, f.nome_fascicolo " +
                            "FROM " + getTitoliTable() + " t, " + getFascicoliTable() + " f, " + getFascicoliTable() + " ff " +
                            "WHERE f.id_fascicolo = ? " + 
                            "AND f.id_fascicolo_padre = ff.id_fascicolo " + 
                            "AND t.id_titolo = (SELECT fgg.id_titolo " + 
                                                "FROM " + getFascicoliTable() + " fg, " + getFascicoliTable() + " fgg " +
                                                "WHERE fg.id_fascicolo = ? " + 
                                                "AND fg.id_fascicolo_padre = fgg.id_fascicolo)";
	
                    try (PreparedStatement ps = dbConn.prepareStatement(sqlTextL3)) {
                        ps.setString(1, idFascicoloPadre);
                        ps.setString(2, idFascicoloPadre);
                        log.debug("eseguo la query: " + ps.toString() + " ...");
                        ResultSet resL1 = ps.executeQuery();

                        if (!resL1.next())
                            throw new SQLException("titolo non trovato");
                        else{
                            int index = 1;
                            String classifica = resL1.getString(index++);
                            int annoF =  resL1.getInt(index++);
                            int numeroF =  resL1.getInt(index++);
                            String nomeF = resL1.getString(index++);
                            int numeroS = resL1.getInt(index++);
                            String nomeS = resL1.getString(index++);
                            
                            classifica = classifica.replaceAll("-", "/");
                            res.setClassificaFascicolo(classifica);
                            
                            res.setAnnoFascicolo(annoF);
                            res.setNomeFascicolo(nomeF);
                            res.setNumeroFasciolo(numeroF);
                            
                            res.setNomeSottoFascicolo(nomeS);
                            res.setNumeroSottoFascicolo(numeroS);
                            
                            res.setNomeInserto(nomeFascicolo);
                            res.setNumeroInserto(numeroFascicolo);
                            res.setLevel(3);
                        }
                    }
                break;  
            }
        }
        
        return res;    
    }
    
    
    public String getNomeCognome(Connection dbConn, String idUtente) throws SQLException{
        
        String result = null;
        
        String sqlText = 
                "SELECT nome, cognome " +
                "FROM " + getUtentiTable() + " " +
                "WHERE id_utente = ? ";          

        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, idUtente);
            log.debug("eseguo la query: " + ps.toString() + " ...");
            ResultSet res = ps.executeQuery();

            if (!res.next())
                throw new SQLException("utente non trovato");
            else
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
    
    public String getTitoliVersioniTable() {
        return titoliVersioniTable;
    }
    
    public String getTitoliVersioniCrossTable() {
        return titoliVersioniCrossTable;
    }
    
    public String getFascicoliTable() {
        return fascicoliTable;
    }
    
    public String getFascicoliGdDocTable() {
        return fascicoliGdDocTable;
    }
    
    public String getUtentiTable() {
        return utentiTable;
    }
}
