package com.dk0124.migration.task;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.persistence.repositoryUtils.bithumb.candle.BithumbCandleRepositoryUtils;
import com.dk0124.migration.migration.candles.BithumbCandleMigration;
import com.dk0124.migration.migration.ticks.BithumbTickMigration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class MigrationTask implements ApplicationRunner {

	private final BithumbCandleMigration BCMigration;
	private final BithumbTickMigration bithumbTickMigration;

	@Override
	public void run(ApplicationArguments args) throws Exception {

		log.info("DO  : " + System.currentTimeMillis());
		log.info("\n***********RUN BITHUMB CANDLE***********");
		run_bithumb_candle();

		log.info("\n***********RUN BITHUMB TICK***********");
		run_bithumb_tick();

		log.info("DONE  : " + System.currentTimeMillis());

	}

	private void run_bithumb_candle() {
		for (BithumbCoinCode code : BithumbCoinCode.values()) {
			log.info(" migrate : {} ", code.toString());
			BCMigration.setPGRepo(code);
			BCMigration.readyMigrate(code, 2000);
			BCMigration.migrate(code);
		}
	}

	private void run_bithumb_tick() {
		for (BithumbCoinCode code : BithumbCoinCode.values()) {
			log.info(" migrate : {} ", code.toString());
			bithumbTickMigration.setPGRepo(code);
			bithumbTickMigration.readyMigrate(code, 2000);
			bithumbTickMigration.migrate(code);
		}
	}
}
