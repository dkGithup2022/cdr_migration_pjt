package com.dk0124.migration.migration.ticks;

import java.util.List;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.dk0124.cdr.persistence.entity.upbit.tick.UpbitTick;
import com.dk0124.cdr.persistence.repository.upbit.upbitTickRepository.UpbitTickRepository;
import com.dk0124.cdr.persistence.repositoryUtils.upbit.UpbitTickRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class UpbitTickMigration extends AbstarctMigration
	<
		UpbitTick,
		UpbitTickDoc,
		UpbitTickRepository,
		com.dk0124.cdr.es.dao.upbit.UpbitTickRepository,
		UpbitCoinCode
		> {

	private final UpbitTickRepositoryUtils repoPicker;

	public UpbitTickMigration(com.dk0124.cdr.es.dao.upbit.UpbitTickRepository esRepository, ModelMapper modelMapper,
		UpbitTickRepositoryUtils repoPicker) {
		super(esRepository, modelMapper);
		this.repoPicker = repoPicker;
	}

	@Override
	protected void updateTimeStamp(List<UpbitTick> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getTimestamp() + 1;
	}

	@Override
	public UpbitTickDoc mapToDoc(UpbitTick row) {
		return modelMapper.map(row, UpbitTickDoc.class);
	}

	@Override
	public void upsertDoc(UpbitTickDoc doc) {
		esRepository.index(EsUtils.generateIndex(doc), EsUtils.generateId(doc), doc);
	}

	@Override
	public List<UpbitTick> readNextPage(Long cursorTimstamp, Pageable pageable) {
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
