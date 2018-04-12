package org.elastic.doc;

/**
 * Created by Matheus on 07/03/2017.
 */
public class Documento {
    private String id;
    private String valor;
    private String conteudo;
    private String tags;

    
    public Documento(String id, String valor, String conteudo, String tags) {
		super();
		this.id = id;
		this.valor = valor;
		this.conteudo = conteudo;
		this.tags = tags;
	}

	public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValor() {
        return valor;
    }

    public void setValor(String valor) {
        this.valor = valor;
    }

    public String getConteudo() {
        return conteudo;
    }

    public void setConteudo(String conteudo) {
        this.conteudo = conteudo;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }
}
