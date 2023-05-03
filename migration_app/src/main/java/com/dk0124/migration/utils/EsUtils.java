package com.dk0124.migration.utils;

import java.util.Locale;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.document.bithumb.BithumbCandleDoc;
import com.dk0124.cdr.es.document.bithumb.BithumbOrderbookDoc;
import com.dk0124.cdr.es.document.bithumb.BithumbTickDoc;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.dk0124.cdr.persistence.entity.bithumb.orderbook.BithumbOrderbook;

public class EsUtils {

	private static final String UPBIT_TICK_INDEX_PREFIX = "upbit_tick";
	private static final String BITHUMB_TICK_INDEX_PREFIX = "bithumb_tick";
	private static final String UPBIT_CANDLE_INDEX_PREFIX = "upbit_candle";
	private static final String BITHUMB_CANDLE_INDEX_PREFIX = "bithumb_candle";
	private static final String UPBIT_ORDERBOOK_INDEX_PREFIX = "upbit_orderbook";
	private static final String BITHUMB_ORDERBOOK_INDEX_PREFIX = "bihumb_orderbook";

	// 틱 인덱스 생성
	public static String generateIndex(UpbitTickDoc tickDoc) {
		if (UpbitCoinCode.fromString(tickDoc.getCode().toUpperCase(Locale.ROOT)) == null)
			throw new IllegalArgumentException("Invalid market: " + tickDoc.toString());

		String[] splitted = tickDoc.getCode().toLowerCase(Locale.ROOT).split("-");
		return UPBIT_TICK_INDEX_PREFIX + "_" + String.join("_", splitted);
	}

	// 틱 ID 생성
	public static String generateId(UpbitTickDoc tickDoc) {
		String code = tickDoc.getCode().toLowerCase(Locale.ROOT);
		long timestamp = tickDoc.getTimestamp();
		return code + "_" + timestamp;
	}

	// 캔들 인덱스 생성
	public static String generateIndex(UpbitCandleDoc candleDoc) {
		if (UpbitCoinCode.fromString(candleDoc.getMarket().toUpperCase(Locale.ROOT)) == null)
			throw new IllegalArgumentException("Invalid market: " + candleDoc.toString());

		String[] splitted = candleDoc.getMarket().toLowerCase(Locale.ROOT).split("-");
		return UPBIT_CANDLE_INDEX_PREFIX + "_" + String.join("_", splitted);
	}

	// 캔들 ID 생성
	public static String generateId(UpbitCandleDoc candleDoc) {
		String market = candleDoc.getMarket().toLowerCase(Locale.ROOT);
		long timestamp = candleDoc.getTimestamp();
		return market + "_" + timestamp;
	}

	// 오더북 인덱스 생성
	public static String generateIndex(UpbitOrderbookDoc orderbookDoc) {

		if (UpbitCoinCode.fromString(orderbookDoc.getCode().toUpperCase(Locale.ROOT)) == null)
			throw new IllegalArgumentException("Invalid market: " + orderbookDoc.toString());

		String[] splitted = orderbookDoc.getCode().toLowerCase(Locale.ROOT).split("-");
		return UPBIT_ORDERBOOK_INDEX_PREFIX + "_" + String.join("_", splitted);
	}

	// 오더북 ID 생성
	public static String generateId(UpbitOrderbookDoc orderbookDoc) {
		String code = orderbookDoc.getCode().toLowerCase(Locale.ROOT);
		long timestamp = orderbookDoc.getTimestamp();
		return code + "_" + timestamp;
	}

	// 빗섬 캔들 아이디
	public static String generateId(BithumbCandleDoc doc) {
		return doc.getCode() + "_" + doc.getTimestamp();
	}

	// 빗섬 캔들 인덱스
	public static String generateIndex(BithumbCandleDoc doc) {
		String code = doc.getCode();
		if (BithumbCoinCode.fromString(code) == null)
			throw new RuntimeException("INVALID CODE");
		String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");
		return BITHUMB_CANDLE_INDEX_PREFIX + "_" + String.join("_", splitted);
	}

	// 빗섬 캔들 아이디
	public static String generateId(BithumbOrderbookDoc doc) {
		return doc.getCode() + "_" + doc.getDatetime();
	}

	// 빗섬 캔들
	public static String generateIndex(BithumbOrderbookDoc doc) {
		String code = doc.getCode();
		if (BithumbCoinCode.fromString(code) == null)
			throw new RuntimeException("INVALID CODE");
		String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");
		return BITHUMB_ORDERBOOK_INDEX_PREFIX + "_" + String.join("_", splitted);
	}

	// 빗섬 틱
	public static String generateId(BithumbTickDoc doc) {
		return doc.getCode() + "_" + doc.getTimestamp();
	}

	// 빗섬 틱
	public static String generateIndex(BithumbTickDoc doc) {
		String code = doc.getCode();
		if (BithumbCoinCode.fromString(code) == null)
			throw new RuntimeException("INVALID CODE");
		String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");
		return BITHUMB_TICK_INDEX_PREFIX + "_" + String.join("_", splitted);
	}

}
