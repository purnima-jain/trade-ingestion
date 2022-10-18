package com.purnima.jain.trade.ingestion.domain.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.Month;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;

import com.purnima.jain.trade.ingestion.domain.exception.HigherVersionExistsException;
import com.purnima.jain.trade.ingestion.domain.model.Trade;
import com.purnima.jain.trade.ingestion.repo.TradeRepository;

@SpringBootTest
class TradeIngestionServiceImplTest {

	@Mock
	private TradeRepository tradeRepository;

	@InjectMocks
	private TradeIngestionService tradeIngestionService = new TradeIngestionServiceImpl(tradeRepository);

	@DisplayName("ingestTrade_ValidTrade_ShouldGetIngested")
	@Test
	void ingestTrade_ValidTrade_ShouldGetIngested() throws HigherVersionExistsException {
		Trade trade = initValidTrade();

		doNothing().when(tradeRepository).save(any(Trade.class));

		Trade ingestedTrade = tradeIngestionService.ingestTrade(trade);

		assertEquals(trade, ingestedTrade);
		verify(tradeRepository, times(1)).save(trade);
	}

	@DisplayName("ingestTrade_TradeWithMaturityDateInPast_ShouldNotGetIngested")
	@Test
	void ingestTrade_TradeWithMaturityDateInPast_ShouldNotGetIngested() {
		Trade tradeWithMaturityDateBeforeToday = initValidTrade();
		tradeWithMaturityDateBeforeToday.setMaturityDate(LocalDate.of(2014, Month.MAY, 20));

		doNothing().when(tradeRepository).save(any(Trade.class));

		Trade ingestedTrade = tradeIngestionService.ingestTrade(tradeWithMaturityDateBeforeToday);

		assertEquals(null, ingestedTrade);
		verifyNoInteractions(tradeRepository);
	}

	@DisplayName("ingestTrade_TradeWithLowerVersion_ShouldNotGetIngested_ShouldThrowException")
	@Test
	void ingestTrade_TradeWithLowerVersion_ShouldNotGetIngested_ShouldThrowException() {
		Trade trade4WithLowerVersion = initValidTrade();
		trade4WithLowerVersion.setVersion(4);

		when(tradeRepository.getLatestVersion(any(String.class))).thenReturn(trade4WithLowerVersion.getVersion() + 1);

		HigherVersionExistsException higherVersionExistsException = assertThrows(HigherVersionExistsException.class, () -> {
			tradeIngestionService.ingestTrade(trade4WithLowerVersion);
		});

		assertEquals(higherVersionExistsException.getMessage(), "Invalid Input!!! Higher Version Exists for tradeId :: " + trade4WithLowerVersion.getTradeId());
		verify(tradeRepository, times(1)).getLatestVersion(trade4WithLowerVersion.getTradeId());
	}

	@DisplayName("ingestTrade_TradeWithExistingVersion_ShouldOverwriteExistingRow")
	@Test
	void ingestTrade_TradeWithExistingVersion_ShouldOverwriteExistingRow() {
		Trade trade4WithExistingVersion = initValidTrade();
		trade4WithExistingVersion.setVersion(5);
		trade4WithExistingVersion.setCounterpartyId("CP-1_UPDATED");
		trade4WithExistingVersion.setBookId("B1_UPDATED");

		when(tradeRepository.getLatestVersion(any(String.class))).thenReturn(trade4WithExistingVersion.getVersion());

		Trade ingestedTrade = tradeIngestionService.ingestTrade(trade4WithExistingVersion);

		verify(tradeRepository, times(1)).getLatestVersion(trade4WithExistingVersion.getTradeId());
		assertEquals(trade4WithExistingVersion, ingestedTrade);
		verify(tradeRepository, times(1)).save(trade4WithExistingVersion);
	}

	@DisplayName("ingestTrade_TradeWithHigherVersion_ShouldGetIngested")
	@Test
	void ingestTrade_TradeWithHigherVersion_ShouldGetIngested() {
		Trade trade4WithHigherVersion = initValidTrade();
		trade4WithHigherVersion.setVersion(5);

		when(tradeRepository.getLatestVersion(any(String.class))).thenReturn(trade4WithHigherVersion.getVersion() - 1);

		Trade ingestedTrade = tradeIngestionService.ingestTrade(trade4WithHigherVersion);

		verify(tradeRepository, times(1)).getLatestVersion(trade4WithHigherVersion.getTradeId());
		assertEquals(trade4WithHigherVersion, ingestedTrade);
		verify(tradeRepository, times(1)).save(trade4WithHigherVersion);
	}

	@DisplayName("updateMaturedTradeToExpired_ShouldUpdateRows")
	@Test
	void updateMaturedTradeToExpired_ShouldUpdateRows() {
		doNothing().when(tradeRepository).updateMaturedTradeToExpired();

		tradeIngestionService.updateMaturedTradeToExpired();

		verify(tradeRepository, times(1)).updateMaturedTradeToExpired();
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
