package br.pucminas.aed.ingressos.domain;

public final class EventoRetidoVO {

    private final String ceId;
    private final String ceType;
    private final String chave;
    private final String topicoOriginal;
    private final Integer particaoOriginal;
    private final Long offsetOriginal;
    private final String grupoQueFalhou;
    private final String excecao;
    private final String mensagem;
    private final String classificacao;
    private final String falhouEm;
    private final int particaoNaDlq;
    private final long offsetNaDlq;
    private final String carga;

    public EventoRetidoVO(String ceId, String ceType, String chave, String topicoOriginal,
            Integer particaoOriginal, Long offsetOriginal, String grupoQueFalhou, String excecao,
            String mensagem, String classificacao, String falhouEm, int particaoNaDlq, long offsetNaDlq,
            String carga) {
        this.ceId = ceId;
        this.ceType = ceType;
        this.chave = chave;
        this.topicoOriginal = topicoOriginal;
        this.particaoOriginal = particaoOriginal;
        this.offsetOriginal = offsetOriginal;
        this.grupoQueFalhou = grupoQueFalhou;
        this.excecao = excecao;
        this.mensagem = mensagem;
        this.classificacao = classificacao;
        this.falhouEm = falhouEm;
        this.particaoNaDlq = particaoNaDlq;
        this.offsetNaDlq = offsetNaDlq;
        this.carga = carga;
    }

    public String getCeId() {
        return ceId;
    }

    public String getCeType() {
        return ceType;
    }

    public String getChave() {
        return chave;
    }

    public String getTopicoOriginal() {
        return topicoOriginal;
    }

    public Integer getParticaoOriginal() {
        return particaoOriginal;
    }

    public Long getOffsetOriginal() {
        return offsetOriginal;
    }

    public String getGrupoQueFalhou() {
        return grupoQueFalhou;
    }

    public String getExcecao() {
        return excecao;
    }

    public String getMensagem() {
        return mensagem;
    }

    public String getClassificacao() {
        return classificacao;
    }

    public String getFalhouEm() {
        return falhouEm;
    }

    public int getParticaoNaDlq() {
        return particaoNaDlq;
    }

    public long getOffsetNaDlq() {
        return offsetNaDlq;
    }

    public String getCarga() {
        return carga;
    }
}
