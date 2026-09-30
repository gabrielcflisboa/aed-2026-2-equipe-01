package br.pucminas.aed.ingressos.domain;

public sealed interface EstoqueEvent
        permits SetorAbertoEvent, IngressoRetiradoEvent, IngressoDevolvidoEvent, ReservaRecusadaEvent {

    String tipo();
}
