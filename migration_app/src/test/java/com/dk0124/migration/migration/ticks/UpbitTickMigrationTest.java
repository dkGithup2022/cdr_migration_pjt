package com.dk0124.migration.migration.ticks;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.persistence.entity.bithumb.tick.BithumbTick;
import com.dk0124.cdr.persistence.entity.upbit.tick.UpbitTick;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class UpbitTickMigrationTest {

	@Autowired
	private UpbitTickMigration upbitTickMigration;

	@Test
	public void 의존성확인() {
		assertNotNull(upbitTickMigration);
	}

	@Test
	public void PG_읽기_단건() {
		//given
		upbitTickMigration.setPGRepo(UpbitCoinCode.KRW_BAT);

		//when
		List<UpbitTick> list = upbitTickMigration.read();

		//then
		//size
		assertEquals(upbitTickMigration.getBulkSize() + 1, list.size());

		//is sorted;
		assertTrue(descSorted(list));
	}

	private boolean descSorted(List<UpbitTick> candles) {
		Long timestamp = candles.get(0).getTimestamp();
		for (UpbitTick candle : candles)
			if (timestamp >= candle.getTimestamp())
				timestamp = candle.getTimestamp();
			else
				return false;

		return true;
	}

	@Test
	public void PG_여러번_읽기_중복_확인() {

		// given
		upbitTickMigration.setPGRepo(UpbitCoinCode.KRW_BAT);

		// when
		List<UpbitTick> list1 = upbitTickMigration.read();
		upbitTickMigration.updateTimeStamp(list1);
		List<UpbitTick> list2 = upbitTickMigration.read();

		// count document
		HashSet<UpbitTick> set = new HashSet<>();
		list1.stream().forEach(e -> set.add(e));
		list2.stream().forEach(e -> set.add(e));

		assertTrue(upbitTickMigration.getBulkSize() * 2 == set.size());
	}

	@Test
	public void PG_커서_마지막_도달시_endOfPage_확인() {
		// given
		upbitTickMigration.setPGRepo(UpbitCoinCode.KRW_BAT);
		upbitTickMigration.setCursorTimstamp(1670588986912L);

		List<UpbitTick> ticks = upbitTickMigration.read();
		upbitTickMigration.updateTimeStamp(ticks);

		assertTrue(upbitTickMigration.endOfTable(ticks));
	}

	@Test
	public void es_칼럼_매핑_확인() {}
}