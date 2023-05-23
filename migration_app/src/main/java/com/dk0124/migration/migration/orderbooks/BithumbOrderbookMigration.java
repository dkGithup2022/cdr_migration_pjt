package com.dk0124.migration.migration.orderbooks;

import java.util.List;
import java.util.stream.Collectors;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.document.bithumb.BithumbOrderbookDoc;
import com.dk0124.cdr.persistence.entity.bithumb.orderbook.BithumbOrderbook;
import com.dk0124.cdr.persistence.repository.bithumb.bithumbOrderbookRepository.BithumbOrderbookRepository;
import com.dk0124.cdr.persistence.repositoryUtils.bithumb.BithumbOrderbookRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
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
	public void migrate(BithumbCoinCode coinCode) {

		setPGRepo(coinCode);
		log.info("\n\n******************************************************");
		log.info("BEGIN MIGRATION JOB ON {}", coinCode.toString());
		Long iter = 0L;
		while (true) {
			// read from postgres
			List<BithumbOrderbook> rows = read();
			List<BithumbOrderbook> sampled = sampling(rows);
			List<BithumbOrderbookDoc> docs = sampled.stream().map(e -> mapToDoc(e)).collect(Collectors.toList());

			// update total cnt
			migratedCnt += docs.size();

			updateTimeStamp(rows);
			// update timestamp

			// check it is end;
			if (endOfTable(rows))
				break;

			iter++;
			if (iter % 1000 == 0)
				printCurrentTimeStamp();

		}

		log.info("\n\n******************************************************");
		log.info("MIGRATION JOB DONE ON TICK {}", coinCode.toString());
		log.info("TOTAL {} DATA FETCHED ", migratedCnt);
	}

	/**
	 * TODO ->  오더북 마이그레이션 시 1분당 1개로 샘플링 ...
	 * @param rows
	 * @return
	 */
	private List<BithumbOrderbook> sampling(List<BithumbOrderbook> rows) {
		return null;
	}

	@Override
	protected void updateTimeStamp(List<BithumbOrderbook> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getDatetime() + 1;
	}

	@Override
	public BithumbOrderbookDoc mapToDoc(BithumbOrderbook row) {
		/**
		 * TODO
		 *
		 */
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
