package it.bologna.ausl.bds_tools.jobs.utils;

/**
 *
 * @author spritz
 */
public class ServiceRequestInformation {
    
    public enum ModalitaAccesso{
        SCHEDULATO, 
        AMMINISTRAZIONE,
        APPLICAZIONE
    };
    
    private String serviceName;
    private ModalitaAccesso modalitaAccesso;
    private String idUtente;
    private String azione;
    private String motivazioneAzione;
    private Object content;

    public ServiceRequestInformation() {
    }

    public ServiceRequestInformation(String serviceName, ModalitaAccesso modalitaAccesso, String idUtente, String content) {
        this.serviceName = serviceName;
        this.modalitaAccesso = modalitaAccesso;
        this.idUtente = idUtente;
        this.content = content;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public ModalitaAccesso getModalitaAccesso() {
        return modalitaAccesso;
    }

    public void setModalitaAccesso(ModalitaAccesso modalitaAccesso) {
        this.modalitaAccesso = modalitaAccesso;
    }

    public String getIdUtente() {
        return idUtente;
    }

    public void setIdUtente(String idUtente) {
        this.idUtente = idUtente;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public String getAzione() {
        return azione;
    }

    public void setAzione(String azione) {
        this.azione = azione;
    }

    public String getMotivazioneAzione() {
        return motivazioneAzione;
    }

    public void setMotivazioneAzione(String motivazioneAzione) {
        this.motivazioneAzione = motivazioneAzione;
    }
}
