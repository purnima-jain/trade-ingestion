package com.purnima.jain.trade.ingestion.domain.service;

import com.purnima.jain.trade.ingestion.domain.model.Trade;

public interface TradeIngestionService {

	Trade ingestTrade(Trade trade);

	void updateMaturedTradeToExpired();

}
