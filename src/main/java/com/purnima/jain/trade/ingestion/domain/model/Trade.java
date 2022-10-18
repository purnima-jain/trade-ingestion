package com.purnima.jain.trade.ingestion.domain.model;

import java.time.LocalDate;

import lombok.Data;

@Data
public class Trade {

	private String tradeId;
	private Integer version;
	private String counterpartyId;
	private String bookId;
	private LocalDate maturityDate;
	private LocalDate createdDate;
	private Boolean expired;

}
