package it.bologna.ausl.bds_tools.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author gdm
 */
public class Registro {
    private String codiceRegistro;
    private String descrizione;
    private String principale;
    private String tipoDocumento;
    private String descPubbAlbo;
    private String sequenzaAssociata;

    public Registro() {
    }

    public Registro(String codiceRegistro, String descrizione, String principale, String tipoDocumento, String descPubbAlbo, String sequenzaAssociata) {
        this.codiceRegistro = codiceRegistro;
        this.descrizione = descrizione;
        this.principale = principale;
        this.tipoDocumento = tipoDocumento;
        this.descPubbAlbo = descPubbAlbo;
        this.sequenzaAssociata = sequenzaAssociata;
    }

    public String getCodiceRegistro() {
        return codiceRegistro;
    }

    public void setCodiceRegistro(String codiceRegistro) {
        this.codiceRegistro = codiceRegistro;
    }

    public String getDescrizione() {
        return descrizione;
    }

    public void setDescrizione(String descrizione) {
        this.descrizione = descrizione;
    }

    public String getPrincipale() {
        return principale;
    }

    public void setPrincipale(String principale) {
        this.principale = principale;
    }

    public String getTipoDocumento() {
        return tipoDocumento;
    }

    public void setTipoDocumento(String tipoDocumento) {
        this.tipoDocumento = tipoDocumento;
    }

    public String getDescPubbAlbo() {
        return descPubbAlbo;
    }

    public void setDescPubbAlbo(String descPubbAlbo) {
        this.descPubbAlbo = descPubbAlbo;
    }

    public String getSequenzaAssociata() {
        return sequenzaAssociata;
    }

    public void setSequenzaAssociata(String sequenzaAssociata) {
        this.sequenzaAssociata = sequenzaAssociata;
    }

    /**
     * torna un oggetto Registro che rappresenta il registro identificato dal codice registor passato
     * @param codiceRegistro il codice registro del registro
     * @param dbConn
     * @return un oggetto Registro che rappresenta il registro identificato dal codice registor passato
     * @throws SQLException 
     */
    public static Registro getRegistro(String codiceRegistro, Connection dbConn) throws SQLException {
        String sqlText =
                        "SELECT codice_registro, descrizione, principale, tipo_documento, desc_pubb_albo, sequenza_associata " +
                        "FROM " + ApplicationParams.getRegistriTableName() + " " +
                        "WHERE codice_registro = ?";
        Registro r;
        try (PreparedStatement ps = dbConn.prepareStatement(sqlText)) {
            ps.setString(1, codiceRegistro);

            ResultSet res = ps.executeQuery();

            if (res.next()) {
                 r = new Registro(
                        res.getString("codice_registro"),
                        res.getString("descrizione"),
                        res.getString("principale"),
                        res.getString("tipo_documento"),
                        res.getString("desc_pubb_albo"),
                        res.getString("sequenza_associata"));
            }
            else {
                throw new SQLException("Registro non trovato");
            }
        }
        return r;
    }
}
