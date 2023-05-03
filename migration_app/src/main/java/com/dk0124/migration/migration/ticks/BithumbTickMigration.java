package com.dk0124.migration.migration.ticks;

import java.util.List;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.dk0124.cdr.es.document.bithumb.BithumbTickDoc;
import com.dk0124.cdr.persistence.entity.bithumb.tick.BithumbTick;
import com.dk0124.cdr.persistence.repository.bithumb.bithumbTickRepository.BithumbTickRepository;
import com.dk0124.cdr.persistence.repositoryUtils.bithumb.BithumbTickRepositoryUtils;
import com.dk0124.migration.migration.AbstarctMigration;
import com.dk0124.migration.utils.EsUtils;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BithumbTickMigration
	extends AbstarctMigration
	<
		BithumbTick,
		BithumbTickDoc,
		BithumbTickRepository,
		com.dk0124.cdr.es.dao.bithumb.BithumbTickRepository,
		BithumbCoinCode> {
	private final BithumbTickRepositoryUtils repoPicker;

	public BithumbTickMigration(com.dk0124.cdr.es.dao.bithumb.BithumbTickRepository esRepository,
		ModelMapper modelMapper, BithumbTickRepositoryUtils repoPicker) {
		super(esRepository, modelMapper);
		this.repoPicker = repoPicker;

	}

	@Override
	protected void updateTimeStamp(List<BithumbTick> rows) {
		this.cursorTimstamp = rows.get(rows.size() - 2).getTimestamp() + 1;
	}

	@Override
	public BithumbTickDoc mapToDoc(BithumbTick row) {
		return modelMapper.map(row, BithumbTickDoc.class);
	}

	@Override
	public void upsertDoc(BithumbTickDoc doc) {
		esRepository.index(EsUtils.generateIndex(doc), EsUtils.generateId(doc), doc);
	}

	@Override
	public List<BithumbTick> readNextPage(Long cursorTimstamp, Pageable pageable) {
		return rdmsRepo.findByTimestampLessThanEqual(cursorTimstamp,
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


