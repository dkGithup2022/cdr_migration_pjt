package com.dk0124.migration.migration.candles;

import java.util.List;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.document.bithumb.BithumbCandleDoc;
import com.dk0124.cdr.persistence.entity.bithumb.candle.BithumbCandle;
import com.dk0124.cdr.persistence.repository.bithumb.bithumbCandleRepository.BithumbCandleRepository;
import com.dk0124.cdr.persistence.repositoryUtils.bithumb.candle.BithumbCandleRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BithumbCandleMigration extends AbstarctMigration
	<
		BithumbCandle,
		BithumbCandleDoc,
		BithumbCandleRepository,
		com.dk0124.cdr.es.dao.bithumb.BithumbCandleRespository,
		BithumbCoinCode
		> {

	private final BithumbCandleRepositoryUtils repoPicker;

	public BithumbCandleMigration(com.dk0124.cdr.es.dao.bithumb.BithumbCandleRespository esRepository,
		ModelMapper modelMapper, BithumbCandleRepositoryUtils repoPicker) {
		super(esRepository, modelMapper);
		this.repoPicker = repoPicker;
	}

	@Override
	protected void updateTimeStamp(List<BithumbCandle> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getTimestamp() + 1;
	}

	@Override
	public BithumbCandleDoc mapToDoc(BithumbCandle row) {
		return modelMapper.map(row, BithumbCandleDoc.class);
	}

	@Override
	public void upsertDoc(BithumbCandleDoc doc) {
		esRepository.index(EsUtils.generateIndex(doc), EsUtils.generateId(doc), doc);
	}

	@Override
	public List<BithumbCandle> readNextPage(Long cursorTimstamp, Pageable pageable) {
		return rdmsRepo.findByTimestampLessThanEqual(cursorTimstamp,
				PageRequest.of(0, bulkSize + 1, Sort.by("timestamp").descending()))
			.stream()
			.toList();
	}


	@Override
	public void setPGRepo(BithumbCoinCode coinCode) {
		rdmsRepo = repoPicker.getRepositoryFromCode(coinCode);
		this.coinCode = coinCode;
	}

}
