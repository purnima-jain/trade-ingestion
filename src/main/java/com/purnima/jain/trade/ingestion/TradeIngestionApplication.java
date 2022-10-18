package com.purnima.jain.trade.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
@EnableScheduling
public class TradeIngestionApplication {

	public static void main(String[] args) {
		SpringApplication.run(TradeIngestionApplication.class, args);
		log.info("TradeIngestionApplication Started........");
	}

}
