package com.dk0124.migration.migration.candles;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.persistence.entity.bithumb.candle.BithumbCandle;
import com.dk0124.cdr.persistence.entity.bithumb.candle.coins.BithumbCandleKrwBat;
import com.dk0124.cdr.persistence.repository.bithumb.bithumbCandleRepository.BithumbCandleKrwBatRepository;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class BithumbCandleMigrationTest {

	@Autowired
	private BithumbCandleMigration bithumbCandleMigration;

	@Test
	public void 의존성확인() {
		assertNotNull(bithumbCandleMigration);
	}

	@Test
	public void repository_설정확인() {
		bithumbCandleMigration.setPGRepo(BithumbCoinCode.KRW_BAT);
		assertTrue(bithumbCandleMigration.getRdmsRepo() instanceof BithumbCandleKrwBatRepository);
	}

	@Test
	public void PG_읽기_단건() {
		//given
		bithumbCandleMigration.setPGRepo(BithumbCoinCode.KRW_BAT);

		//when
		List<BithumbCandle> list = bithumbCandleMigration.read();

		//then

		//size
		assertEquals(bithumbCandleMigration.getBulkSize() + 1, list.size());

		//is sorted;
		assertTrue(descSorted(list));
	}

	private boolean descSorted(List<BithumbCandle> candles) {
		Long timestamp = candles.get(0).getTimestamp();
		for (BithumbCandle candle : candles)
			if (timestamp >= candle.getTimestamp())
				timestamp = candle.getTimestamp();
			else
				return false;

		return true;
	}

	@Test
	public void PG_여러번_읽기_중복_확인() {

		// given
		bithumbCandleMigration.setPGRepo(BithumbCoinCode.KRW_BAT);

		// when
		List<BithumbCandle> list1 = bithumbCandleMigration.read();
		bithumbCandleMigration.updateTimeStamp(list1);
		List<BithumbCandle> list2 = bithumbCandleMigration.read();

		// count document
		HashSet<BithumbCandle> set = new HashSet<>();
		list1.stream().forEach(e -> set.add(e));
		list2.stream().forEach(e -> set.add(e));

		assertTrue(bithumbCandleMigration.getBulkSize() * 2 == set.size());
	}

	@Test
	public void PG_커서_마지막_도달시_endOfPage_확인() {
		// given
		bithumbCandleMigration.setPGRepo(BithumbCoinCode.KRW_BAT);
		bithumbCandleMigration.setCursorTimstamp(1669720140000L);

		List<BithumbCandle> canldes = bithumbCandleMigration.read();
		bithumbCandleMigration.updateTimeStamp(canldes);

		assertTrue(bithumbCandleMigration.endOfTable(canldes));
	}

	@Test
	public void es_칼럼_매핑_확인() {

	}
}