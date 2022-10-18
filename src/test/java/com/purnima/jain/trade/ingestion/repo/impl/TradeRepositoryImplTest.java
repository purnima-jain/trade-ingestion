package com.purnima.jain.trade.ingestion.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.jdbc.DataJdbcTest;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.purnima.jain.trade.ingestion.domain.model.Trade;
import com.purnima.jain.trade.ingestion.repo.TradeRepository;

@DataJdbcTest
class TradeRepositoryImplTest {

	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	private TradeRepository tradeRepository;

	@Autowired
	public TradeRepositoryImplTest(JdbcTemplate jdbcTemplate) {
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate.getDataSource());
		this.tradeRepository = new TradeRepositoryImpl(namedParameterJdbcTemplate);
	}

	@DisplayName("save_ValidTrade_ShouldGetPersisted")
	@Test
	void save_ValidTrade_ShouldGetPersisted() {
		Trade trade = initValidTrade();
		tradeRepository.save(trade);

		SqlParameterSource namedParameters = new MapSqlParameterSource().addValue("tradeId", trade.getTradeId());
		Trade ingestedTrade = (Trade) namedParameterJdbcTemplate.queryForObject("SELECT * FROM trade WHERE trade_id = :tradeId", namedParameters, new BeanPropertyRowMapper<Trade>(Trade.class));

		assertEquals(trade, ingestedTrade);
	}

	@DisplayName("getLatestVersion_ValidTradeId_ShouldRetrieveLatestVersion")
	@Test
	void getLatestVersion_ValidTradeId_ShouldRetrieveLatestVersion() {
		Trade trade = initValidTrade();
		trade.setVersion(Integer.MAX_VALUE);
		tradeRepository.save(trade);

		Integer latestVersion = tradeRepository.getLatestVersion(trade.getTradeId());

		assertEquals(trade.getVersion(), latestVersion);
	}

	@DisplayName("getLatestVersion_InvalidTradeId_ShouldRetrieveNull")
	@Test
	void getLatestVersion_InvalidTradeId_ShouldRetrieveNull() {
		String tradeId = "some_random_trade_id";

		Integer latestVersion = tradeRepository.getLatestVersion(tradeId);

		assertNull(latestVersion);
	}

	@DisplayName("updateMaturedTradeToExpired_TradeIdWithMaturityDateInThePast_ExpiredShouldGetUpdatedToFalse")
	@Test
	void updateMaturedTradeToExpired_TradeIdWithMaturityDateInThePast_ExpiredShouldGetUpdatedToFalse() {
		Trade trade1 = initValidTrade();
		tradeRepository.save(trade1);

		tradeRepository.updateMaturedTradeToExpired();

		Set<String> tradeIds = new HashSet<>(Arrays.asList("T1", "T2", trade1.getTradeId()));
		MapSqlParameterSource parameters = new MapSqlParameterSource();
		parameters.addValue("tradeIds", tradeIds);

		List<Trade> tradeList = namedParameterJdbcTemplate.query("SELECT * FROM trade WHERE trade_id IN (:tradeIds)", parameters, new BeanPropertyRowMapper<Trade>(Trade.class));

		assertEquals(tradeIds.size(), tradeList.size());

		for (Trade trade : tradeList) {
			if (trade.getTradeId().equals(trade1.getTradeId())) {
				assertFalse(trade.getExpired());
				continue;
			}
			assertTrue(trade.getExpired());
		}

	}

	private Trade initValidTrade() {
		Trade trade = new Trade();
		trade.setTradeId("T3");
		trade.setVersion(1);
		trade.setCounterpartyId("CP-1");
		trade.setBookId("B1");
		trade.setMaturityDate(LocalDate.of(2024, Month.MAY, 20));
		trade.setCreatedDate(LocalDate.now());
		trade.setExpired(Boolean.FALSE);

		return trade;
	}

}
