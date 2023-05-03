package com.dk0124.migration.migration.orderbooks;

import java.util.List;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.dao.bithumb.BithumbOrderbookResposirtory;
import com.dk0124.cdr.es.document.bithumb.BithumbOrderbookDoc;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.dk0124.cdr.persistence.entity.bithumb.orderbook.BithumbOrderbook;
import com.dk0124.cdr.persistence.repository.bithumb.bithumbOrderbookRepository.BithumbOrderbookRepository;
import com.dk0124.cdr.persistence.repositoryUtils.bithumb.BithumbOrderbookRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;

public class BithumbOrderbookMigration extends AbstarctMigration
	<
		BithumbOrderbook,
		BithumbOrderbookDoc,
		BithumbOrderbookRepository,
		com.dk0124.cdr.es.dao.bithumb.BithumbOrderbookResposirtory,
		BithumbCoinCode
		> {

	private final BithumbOrderbookRepositoryUtils repoPicker;

	protected BithumbOrderbookMigration(com.dk0124.cdr.es.dao.bithumb.BithumbOrderbookResposirtory esRepository,
		ModelMapper modelMapper, BithumbOrderbookRepositoryUtils repoPicker) {
		super(esRepository, modelMapper);
		this.repoPicker = repoPicker;
	}

	@Override
	protected void updateTimeStamp(List<BithumbOrderbook> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getDatetime() + 1;
	}

	@Override
	public BithumbOrderbookDoc mapToDoc(BithumbOrderbook row) {
		return modelMapper.map(row, BithumbOrderbookDoc.class);
	}

	@Override
	public void upsertDoc(BithumbOrderbookDoc doc) {
		esRepository.index(EsUtils.generateIndex(doc), EsUtils.generateId(doc), doc);
	}

	@Override
	public List<BithumbOrderbook> readNextPage(Long cursorTimstamp, Pageable pageable) {
		return rdmsRepo.findByDatetimeLessThanEqual(cursorTimstamp,
				PageRequest.of(0, bulkSize + 1, Sort.by("timestamp").descending()))
			.stream()
			.toList();
	}

	@Override
	public void setPGRepo(BithumbCoinCode bithumbCoinCode) {
		rdmsRepo = repoPicker.getRepositoryFromCode(bithumbCoinCode);
		this.coinCode = bithumbCoinCode;
	}
}
