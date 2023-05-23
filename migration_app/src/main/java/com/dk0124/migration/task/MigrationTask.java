package com.dk0124.migration.task;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.persistence.repositoryUtils.bithumb.candle.BithumbCandleRepositoryUtils;
import com.dk0124.migration.migration.candles.BithumbCandleMigration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class MigrationTask implements ApplicationRunner {

	private final BithumbCandleMigration BCMigration;
	private final BithumbCandleRepositoryUtils repoPicker;

	@Override
	public void run(ApplicationArguments args) throws Exception {

		for (BithumbCoinCode code : BithumbCoinCode.values()) {
			log.info(" migrate : {} ", code.toString());
			BCMigration.setPGRepo(code);
			BCMigration.readyMigrate(code, 500);
			BCMigration.migrate(code);
		}
	}
}
