package br.pucminas.aed.ingressos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ServicoIngressosApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServicoIngressosApplication.class, args);
	}

}
