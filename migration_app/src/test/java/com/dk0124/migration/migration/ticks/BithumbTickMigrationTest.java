package com.dk0124.migration.migration.ticks;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.persistence.entity.bithumb.tick.BithumbTick;

@SpringBootTest
@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = "app.scheduling.enable=false")
class BithumbTickMigrationTest {

	@Autowired
	private BithumbTickMigration bithumbTickMigration;

	@Test
	public void 의존성확인() {
		assertNotNull(bithumbTickMigration);
	}

	@Test
	public void PG_읽기_단건() {
		//given
		bithumbTickMigration.setPGRepo(BithumbCoinCode.KRW_BAT);

		//when
		List<BithumbTick> list = bithumbTickMigration.read();

		//then

		//size
		assertEquals(bithumbTickMigration.getBulkSize() + 1, list.size());

		//is sorted;
		assertTrue(descSorted(list));
	}

	private boolean descSorted(List<BithumbTick> candles) {
		Long timestamp = candles.get(0).getTimestamp();
		for (BithumbTick candle : candles)
			if (timestamp >= candle.getTimestamp())
				timestamp = candle.getTimestamp();
			else
				return false;

		return true;
	}

	@Test
	public void PG_여러번_읽기_중복_확인() {

		// given
		bithumbTickMigration.setPGRepo(BithumbCoinCode.KRW_BAT);

		// when
		List<BithumbTick> list1 = bithumbTickMigration.read();
		bithumbTickMigration.updateTimeStamp(list1);
		List<BithumbTick> list2 = bithumbTickMigration.read();

		// count document
		HashSet<BithumbTick> set = new HashSet<>();
		list1.stream().forEach(e -> set.add(e));
		list2.stream().forEach(e -> set.add(e));

		assertTrue(bithumbTickMigration.getBulkSize() * 2 == set.size());
	}

	@Test
	@Disabled // 잠시
	public void PG_커서_마지막_도달시_endOfPage_확인() {
		// given
		bithumbTickMigration.setPGRepo(BithumbCoinCode.KRW_BAT);
		bithumbTickMigration.setCursorTimstamp(1669720140000L);
		bithumbTickMigration.setBulkSize(3000);

		List<BithumbTick> canldes = bithumbTickMigration.read();
		bithumbTickMigration.updateTimeStamp(canldes);

		assertTrue(bithumbTickMigration.endOfTable(canldes));
	}

	@Test
	public void es_칼럼_매핑_확인() {

	}

}