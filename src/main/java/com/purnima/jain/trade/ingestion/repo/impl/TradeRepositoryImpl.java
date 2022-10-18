package com.purnima.jain.trade.ingestion.repo.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import com.purnima.jain.trade.ingestion.domain.model.Trade;
import com.purnima.jain.trade.ingestion.repo.TradeRepository;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class TradeRepositoryImpl implements TradeRepository {

	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	private final static String UPSERT_QUERY = "MERGE INTO trade KEY (trade_id, version) VALUES (:tradeId, :version, :counterpartyId, :bookId, :maturityDate, :createdDate, :expired)";
	private final static String MAX_VERSION_QUERY = "SELECT MAX(version) FROM trade WHERE trade_id = :tradeId";
	private final static String UPDATE_MATURED_TRADE_TO_EXPIRED_QUERY = "UPDATE trade SET expired = TRUE WHERE MATURITY_DATE < CURRENT_DATE() AND expired = FALSE";

	@Autowired
	public TradeRepositoryImpl(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}

	@Override
	public void save(Trade trade) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("tradeId", trade.getTradeId());
		paramMap.put("version", trade.getVersion());
		paramMap.put("counterpartyId", trade.getCounterpartyId());
		paramMap.put("bookId", trade.getBookId());
		paramMap.put("maturityDate", trade.getMaturityDate());
		paramMap.put("createdDate", trade.getCreatedDate());
		paramMap.put("expired", trade.getExpired());

		Integer countOfRowsInserted = namedParameterJdbcTemplate.update(UPSERT_QUERY, paramMap);
		log.info("No of rows inserted in the database: {}", countOfRowsInserted);
	}

	@Override
	public Integer getLatestVersion(String tradeId) {
		SqlParameterSource namedParameters = new MapSqlParameterSource().addValue("tradeId", tradeId);
		Object highestVersionInDbObj = namedParameterJdbcTemplate.queryForObject(MAX_VERSION_QUERY, namedParameters, Object.class);
		Integer highestVersionInDb = null;
		if (highestVersionInDbObj != null) {
			// Cast it to Integer
			highestVersionInDb = (Integer) highestVersionInDbObj;
		}
		log.info("highestVersionInDb :: {}", highestVersionInDb);
		return highestVersionInDb;
	}

	@Override
	public void updateMaturedTradeToExpired() {
		SqlParameterSource namedParameters = new MapSqlParameterSource();
		Integer countOfRowsExpired = namedParameterJdbcTemplate.update(UPDATE_MATURED_TRADE_TO_EXPIRED_QUERY, namedParameters);
		log.info("No of rows impacted in the database: {}", countOfRowsExpired);
	}

}
