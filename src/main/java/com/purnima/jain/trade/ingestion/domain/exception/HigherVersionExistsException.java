package com.purnima.jain.trade.ingestion.domain.exception;

public class HigherVersionExistsException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public HigherVersionExistsException(String message) {
		super(message);
	}

}
