package com.dk0124.migration.migration.orderbooks;

import java.util.List;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.persistence.entity.upbit.orderbook.UpbitOrderbook;
import com.dk0124.cdr.persistence.repository.upbit.upbitOrderBookRepository.UpbitOrderbookRepository;
import com.dk0124.cdr.persistence.repositoryUtils.upbit.UpbitOrderbookRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class UpbitOrderbookMigration extends AbstarctMigration
	<
		UpbitOrderbook,
		UpbitOrderbookDoc,
		UpbitOrderbookRepository,
		com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository,
		UpbitCoinCode
		> {

	private final UpbitOrderbookRepositoryUtils repoPicker;

	protected UpbitOrderbookMigration(com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository esRepository,
		ModelMapper modelMapper,
		UpbitOrderbookRepositoryUtils repoPicker) {
		super(esRepository, modelMapper);
		this.repoPicker = repoPicker;
	}

	@Override
	protected void updateTimeStamp(List<UpbitOrderbook> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getTimestamp() + 1;
	}

	@Override
	public UpbitOrderbookDoc mapToDoc(UpbitOrderbook row) {
		return modelMapper.map(row, UpbitOrderbookDoc.class);
	}

	@Override
	public void upsertDoc(UpbitOrderbookDoc doc) {
		esRepository.index(EsUtils.generateIndex(doc), EsUtils.generateId(doc), doc);
	}

	@Override
	public List<UpbitOrderbook> readNextPage(Long cursorTimstamp, Pageable pageable) {
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
