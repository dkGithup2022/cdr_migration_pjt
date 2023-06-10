package com.dk0124.migration.migration.candles;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.persistence.entity.upbit.candle.UpbitCandle;
import com.dk0124.cdr.persistence.repository.upbit.upbitCandleRepository.UpbitCandleRepository;
import com.dk0124.cdr.persistence.repositoryUtils.upbit.UpbitCandleRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class UpbitCandleMigration extends AbstarctMigration
	<
		UpbitCandle,
		UpbitCandleDoc,
		UpbitCandleRepository,
		com.dk0124.cdr.es.dao.upbit.UpbitCandleRepository,
		UpbitCoinCode> {
	private final UpbitCandleRepositoryUtils repoPicker;

	public UpbitCandleMigration(com.dk0124.cdr.es.dao.upbit.UpbitCandleRepository esRepository,
		ModelMapper modelMapper, UpbitCandleRepositoryUtils repoPicker) {
		super(esRepository, modelMapper);
		this.repoPicker = repoPicker;
	}

	@Override
	protected void updateTimeStamp(List<UpbitCandle> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getTimestamp() + 1;
	}

	@Override
	protected void bulkInsert(List<UpbitCandleDoc> docs) throws JsonProcessingException {
		esRepository.bulkIndex(docs);
	}

	@Override
	public UpbitCandleDoc mapToDoc(UpbitCandle row) {
		UpbitCandleDoc doc = modelMapper.map(row, UpbitCandleDoc.class);
		LocalDateTime utc = LocalDateTime.ofInstant(row.getCandleDateTimeUtc().toInstant(), ZoneId.of("UTC"));
		LocalDateTime kst = LocalDateTime.ofInstant(row.getCandleDateTimeUtc().toInstant(), ZoneId.of("Asia/Seoul"));

		doc.setCandleDateTimeUtc(utc);
		doc.setCandleDateTimeKst(kst);
		return doc;
	}

	@Override
	public void upsertDoc(UpbitCandleDoc doc) {
		esRepository.index(EsUtils.generateIndex(doc), EsUtils.generateId(doc), doc);
	}

	@Override
	public List<UpbitCandle> readNextPage(Long cursorTimstamp, Pageable pageable) {
		return rdmsRepo.findByTimestampLessThanEqual(cursorTimstamp,
				PageRequest.of(0, bulkSize + 1, Sort.by("timestamp").descending()))
			.stream()
			.toList();
	}

	@Override
	public void setPGRepo(UpbitCoinCode coinCode) {
		rdmsRepo = repoPicker.getRepositoryFromCode(coinCode);
		this.coinCode = coinCode;
	}
}


