package com.dk0124.migration.migration.candles;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.persistence.entity.upbit.candle.UpbitCandle;
import com.dk0124.cdr.persistence.repository.upbit.upbitCandleRepository.UpbitCandleKrwBatRepository;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class UpbitCandleMigrationTest {

	@Autowired
	UpbitCandleMigration upbitCandleMigration;

	@Test
	public void 의존성확인() {
		assertNotNull(upbitCandleMigration);
	}

	@Test
	public void repository_설정확인() {
		upbitCandleMigration.setPGRepo(UpbitCoinCode.KRW_BAT);
		assertTrue(upbitCandleMigration.getRdmsRepo() instanceof UpbitCandleKrwBatRepository);
	}

	@Test
	public void PG_읽기_단건() {
		//given
		upbitCandleMigration.setPGRepo(UpbitCoinCode.KRW_BAT);

		//when
		List<UpbitCandle> list = upbitCandleMigration.read();

		//then : check size & sorted desc

		assertEquals(upbitCandleMigration.getBulkSize() + 1, list.size());
		assertTrue(descSorted(list));
	}

	@Test
	public void PG_여러번_읽기_중복_확인() {

		// given
		upbitCandleMigration.setPGRepo(UpbitCoinCode.KRW_BAT);

		// when
		List<UpbitCandle> list1 = upbitCandleMigration.read();
		upbitCandleMigration.updateTimeStamp(list1);
		List<UpbitCandle> list2 = upbitCandleMigration.read();

		// count document
		HashSet<UpbitCandle> set = new HashSet<>();
		for (var c1 : list1)
			set.add(c1);
		for (var c2 : list2)
			set.add(c2);

		assertTrue(upbitCandleMigration.getBulkSize() * 2 == set.size());
	}

	@Test
	public void PG_커서_마지막_도달시_endOfPage_확인() {
		// given
		upbitCandleMigration.setPGRepo(UpbitCoinCode.KRW_BAT);
		upbitCandleMigration.setCursorTimstamp(1670589077656L);

		List<UpbitCandle> canldes = upbitCandleMigration.read();
		upbitCandleMigration.updateTimeStamp(canldes);

		assertTrue(upbitCandleMigration.endOfTable(canldes));
	}

	@Test
	public void es_칼럼_매핑_확인(){

	}

	private boolean descSorted(List<UpbitCandle> candles) {
		Long timestamp = candles.get(0).getTimestamp();
		for (UpbitCandle candle : candles)
			if (timestamp >= candle.getTimestamp())
				timestamp = candle.getTimestamp();
			else
				return false;

		return true;
	}

}