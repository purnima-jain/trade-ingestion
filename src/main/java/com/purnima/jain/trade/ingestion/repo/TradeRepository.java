package com.purnima.jain.trade.ingestion.repo;

import com.purnima.jain.trade.ingestion.domain.model.Trade;

public interface TradeRepository {

	void save(Trade trade);

	Integer getLatestVersion(String tradeId);

	void updateMaturedTradeToExpired();

}
