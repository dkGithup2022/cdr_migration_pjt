package com.dk0124.migration.migration.EsUtils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Locale;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.EsUtils;
import com.dk0124.cdr.es.dao.bithumb.BithumbCandleRespository;
import com.dk0124.cdr.es.dao.bithumb.BithumbOrderbookResposirtory;
import com.dk0124.cdr.es.dao.bithumb.BithumbTickRepository;
import com.dk0124.cdr.es.dao.upbit.UpbitCandleRepository;
import com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository;
import com.dk0124.cdr.es.dao.upbit.UpbitTickRepository;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookUnit;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;

import lombok.extern.slf4j.Slf4j;

/**
 * spring data elasticsearch 에서 @Document 없이 매핑하는 경우 칼럼에 null  생기는 경우 있음.
 * 연산 후 null check 하는 함수 제작 필요 .
 * <p>
 * -> 아래 결과 index with id & findAll 에 null 값 없음 확인 .
 */

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
public class EsMappingTest {

	private static EsIndexOps esIndexOps = new EsIndexOps();

	@Autowired
	BithumbOrderbookResposirtory bithumbOrderbookResposirtory;
	@Autowired
	BithumbCandleRespository bithumbCandleRespository;
	@Autowired
	BithumbTickRepository bithumbTickRepository;

	@Autowired
	UpbitCandleRepository upbitCandleRepository;
	@Autowired
	UpbitOrderbookRepository upbitOrderbookRepository;
	@Autowired
	UpbitTickRepository upbitTickRepository;

	static String[][] PAIR = new String[][] {{"bithumb", "candle"}, {"bithumb", "tick"}, {
		"bithumb", "orderbook"}, {
		"upbit", "candle"}, {"upbit", "tick"}, {"upbit", "orderbook"}};

	@BeforeAll
	public static void re_create_index() throws IOException {
		for (String[] pair : PAIR) {
			String vendor = pair[0];
			String type = pair[1];
			String sp = "elastic/" + vendor + "/" + type + "_setting.json";
			String mp = "elastic/" + vendor + "/" + type + "_mapping.json";
			String prefix = vendor + "_" + type + "_";

			if (vendor.equals("bithumb")) {
				for (BithumbCoinCode code : BithumbCoinCode.values()) {
					String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");
					String idx = "";
					if (type.equals("tick")) {
						idx = EsUtils.bithumbTickIndexFromCode(code.toString());
					} else if (type.equals("orderbook")) {
						idx = EsUtils.bithumbOrderbookIndexFromCode(code.toString());
					} else if (type.equals("candle")) {
						idx = EsUtils.bithumbCandleIndexFromCode(code.toString());
					}

					if (idx.equals(""))
						throw new RuntimeException("error");
					esIndexOps.deleteIndex(idx);
					//esIndexOps.forceMergeAll();
					esIndexOps.createIndexWithMappingAndSetting(idx, mp, sp);
					//esIndexOps.forceMerge(idx);
					log.info("INDEX CREATED : " + idx);
				}
			} else {
				for (UpbitCoinCode code : UpbitCoinCode.values()) {
					String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");

					String idx = "";
					if (type.equals("tick")) {
						idx = EsUtils.upbitTickIndexFromCode(code.toString());
					} else if (type.equals("orderbook")) {
						idx = EsUtils.upbitOrderbookIndexFromCode(code.toString());
					} else if (type.equals("candle")) {
						idx = EsUtils.upbitCandleIndexFromCode(code.toString());
					}

					if (idx.equals(""))
						throw new RuntimeException("error");
					esIndexOps.deleteIndex(idx);
					//esIndexOps.forceMergeAll();
					esIndexOps.createIndexWithMappingAndSetting(idx, mp, sp);
					//esIndexOps.forceMerge(idx);
					log.info("INDEX CREATED : " + idx);
				}

			}
		}
	}

	@Test
	public void recreate_index_() {

	}

	@Test
	@DisplayName(" null check :  upbit tick ")
	public void null_check_upbit_candle() throws InterruptedException {
		// given
		UpbitCoinCode code = UpbitCoinCode.KRW_ADA;
		String index = "upbit_tick_krw_ada";

		for (int i = 0; i < 10; i++) {
			UpbitTickDoc doc = UpbitTickDoc.builder()
				.sequentialId(1L)
				.askBid("ask")
				.change("c")
				.changePrice(10.0)
				.code("krw_ada")
				.prevClosingPrice(10.0)
				.streamType("soc")
				.timestamp(0L + i)
				.tradeDateUtc(LocalDate.now())
				.tradeTimeUtc(LocalTime.MIN)
				.tradePrice(10.0)
				.tradeVolume(10.0)
				.tradeTimestamp(0L + i)
				.type("sell")
				.build();
			String id = "" + i;
			UpbitTickDoc indexed = upbitTickRepository.index(index, id, doc);
		}

		Thread.sleep(1000);

		Pageable pageable = PageRequest.of(0, 3);

		// when
		Page<UpbitTickDoc> res = upbitTickRepository.findAll(index, pageable);

		//then
		for (UpbitTickDoc doc : res.getContent()) {
			assertNotNull(doc.getSequentialId());
			assertNotNull(doc.getCode());
			assertNotNull(doc.getType());
			assertNotNull(doc.getTimestamp());
			assertNotNull(doc.getAskBid());
			assertNotNull(doc.getChange());
			assertNotNull(doc.getTimestamp());
			assertNotNull(doc.getChangePrice());
			assertNotNull(doc.getPrevClosingPrice());
			assertNotNull(doc.getTradeDateUtc());
			assertNotNull(doc.getTradeTimeUtc());
			assertNotNull(doc.getTradeTimestamp());

		}
	}

	@Test
	@DisplayName(" null check :  upbit candle ")
	public void null_check_upit_candle() throws InterruptedException {
		// given
		UpbitCoinCode code = UpbitCoinCode.KRW_ADA;
		String index = "upbit_candle_krw_ada";

		for (int i = 0; i < 10; i++) {
			UpbitCandleDoc doc = UpbitCandleDoc.builder()
				.candleAccTradePrice(10.0)
				.timestamp(0L + i)
				.market(code.toString())
				.candleDateTimeUtc(LocalDateTime.now())
				.candleDateTimeKst(LocalDateTime.now())
				.candleAccTradeVolume(10.0)
				.candleAccTradePrice(10.0)
				.openingPrice(10.0)
				.highPrice(10.0)
				.lowPrice(10.0)
				.tradePrice(10.0)
				.build();
			String id = "" + i;
			UpbitCandleDoc indexed = upbitCandleRepository.index(index, id, doc);
		}

		Thread.sleep(1000);

		Pageable pageable = PageRequest.of(0, 3);

		// when
		Page<UpbitCandleDoc> res = upbitCandleRepository.findAll(index, pageable);

		//then
		for (UpbitCandleDoc doc : res.getContent()) {
			assertNotNull(doc.getTimestamp());
			assertNotNull(doc.getMarket());
			assertNotNull(doc.getCandleAccTradePrice());
			assertNotNull(doc.getCandleAccTradeVolume());
			assertNotNull(doc.getHighPrice());
			assertNotNull(doc.getLowPrice());
			assertNotNull(doc.getCandleDateTimeKst());
			assertNotNull(doc.getCandleDateTimeUtc());

		}
	}

	@Test
	@DisplayName(" null check :  upbit orderbook ")
	public void null_check_upbit_orderbook() throws InterruptedException {
		// given
		UpbitCoinCode code = UpbitCoinCode.KRW_ADA;
		String index = "upbit_orderbook_krw_ada";

		for (int i = 0; i < 10; i++) {
			UpbitOrderbookDoc doc = UpbitOrderbookDoc.builder()
				.code("ada")
				.timestamp(0L + i)
				.totalAskSize(10.0)
				.totalBidSize(10.0)
				.orderBookUnits(Collections.singletonList(
					UpbitOrderbookUnit.builder()
						.askPrice(10.0)
						.bidPrice(10.0)
						.askSize(10.0)
						.bidSize(10.0)
						.build()
				)).build();
			String id = "" + i;
			UpbitOrderbookDoc indexed = upbitOrderbookRepository.index(index, id, doc);
		}

		Thread.sleep(1000);

		Pageable pageable = PageRequest.of(0, 3);

		// when
		Page<UpbitOrderbookDoc> res = upbitOrderbookRepository.findAll(index, pageable);

		//then
		for (UpbitOrderbookDoc doc : res.getContent()) {
			assertNotNull(doc.getCode());
			assertNotNull(doc.getTimestamp());
			assertNotNull(doc.getTotalAskSize());
			assertNotNull(doc.getTotalBidSize());
			assertNotNull(doc.getOrderBookUnits().get(0).getAskPrice());
			assertNotNull(doc.getOrderBookUnits().get(0).getBidPrice());
			assertNotNull(doc.getOrderBookUnits().get(0).getAskSize());
			assertNotNull(doc.getOrderBookUnits().get(0).getBidSize());
		}
	}

}
