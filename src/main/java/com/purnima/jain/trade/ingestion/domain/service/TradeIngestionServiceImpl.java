package com.purnima.jain.trade.ingestion.domain.service;

import java.time.LocalDate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.purnima.jain.trade.ingestion.domain.exception.HigherVersionExistsException;
import com.purnima.jain.trade.ingestion.domain.model.Trade;
import com.purnima.jain.trade.ingestion.repo.TradeRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TradeIngestionServiceImpl implements TradeIngestionService {

	private TradeRepository tradeRepository;

	@Autowired
	public TradeIngestionServiceImpl(TradeRepository tradeRepository) {
		this.tradeRepository = tradeRepository;
	}

	@Override
	public Trade ingestTrade(Trade trade) {

		try {
			if (isValidTrade(trade)) {
				tradeRepository.save(trade);
				return trade;
			}
		} catch (HigherVersionExistsException higherVersionExistsException) {
			log.error(higherVersionExistsException.getMessage());
			higherVersionExistsException.printStackTrace();
			throw higherVersionExistsException;
		}

		return null;
	}

	private Boolean isValidTrade(Trade trade) {
		Boolean isMaturityDateInPast = isMaturityDateInPast(trade);
		if (isMaturityDateInPast) {
			// Ignore the trade silently
			log.info("Trade dropped as it's maturity date is in the past. Trade Id: {}", trade.getTradeId());
			return false;
		}
		Boolean isHigherVersionExists = isHigherVersionExists(trade);
		if (isHigherVersionExists) {
			log.info("Trade dropped as it's higher version already exists in the database. Trade Id: {}", trade.getTradeId());
			throw new HigherVersionExistsException("Invalid Input!!! Higher Version Exists for tradeId :: " + trade.getTradeId());
		}
		return true;

	}

	private Boolean isHigherVersionExists(Trade trade) {
		Integer highestVersionInDb = tradeRepository.getLatestVersion(trade.getTradeId());
		log.info("Latest Version for TradeId: {} is {}", trade.getTradeId(), highestVersionInDb);
		if (highestVersionInDb != null && highestVersionInDb > trade.getVersion()) {
			return true;
		}
		return false;
	}

	private Boolean isMaturityDateInPast(Trade trade) {
		if (trade.getMaturityDate().isBefore(LocalDate.now()))
			return true;
		else
			return false;
	}

	@Scheduled(cron = "${cron.expression.everyday.at.midnight}") // Warning: Do not do this, if running multiple instances of this service
	public void updateMaturedTradeToExpired() {
		tradeRepository.updateMaturedTradeToExpired();
	}

}
