package com.dk0124.migration.migration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import org.modelmapper.ModelMapper;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.dk0124.cdr.es.dao.ElasticsearchRepository;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstarctMigration
		<
		PG_TYPE,
		ES_TYPE,
		PG_REPO,
		ES_REPO extends ElasticsearchRepository,
		COIN_CODE> {

	protected static final Integer MAX_RETRY = 3;

	protected final ES_REPO esRepository;
	protected final ModelMapper modelMapper;

	@Getter
	@Setter
	protected Long cursorTimstamp = Long.MAX_VALUE;

	@Getter
	@Setter
	protected Integer bulkSize = 3000;
	protected COIN_CODE coinCode;
	protected Long migratedCnt = 0L;

	@Getter
	protected PG_REPO rdmsRepo;

	protected AbstarctMigration(ES_REPO esRepository, ModelMapper modelMapper) {
		this.esRepository = esRepository;
		this.modelMapper = modelMapper;
	}

	public void readyMigrate(COIN_CODE coinCode, int bulkSize) {
		this.coinCode = coinCode;
		this.bulkSize = bulkSize;
		this.cursorTimstamp = cursorTimstamp;
	}

	public void migrate(COIN_CODE coinCode) {

		setPGRepo(coinCode);
		log.info("\n\n******************************************************");
		log.info("BEGIN MIGRATION JOB ON {}", coinCode.toString());
		Long iter = 0L;
		while (true) {
			// read from postgres
			List<PG_TYPE> rows = read();
			List<ES_TYPE> docs = rows.stream().map(e -> mapToDoc(e)).collect(Collectors.toList());

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
	 * 기입 대상이 elasticsearch 라 중복제거를 안해도 됨.
	 * id, index 모두 ESUtils 에서 칼럼 값보고 생성해서 기입함..
	 *
	 * @param docs
	 */
	public void fetch(List<ES_TYPE> docs) {
		int retry = 0;
		while (retry < MAX_RETRY) {
			try {
				for (ES_TYPE doc : docs)
					upsertDoc(doc);

			} catch (Exception e) {
				retry++;
				try {
					Thread.sleep(2000);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				log.error(e.getMessage());
			}
		}

		log.error("current status : {}", this.toString());
		throw new RuntimeException("CAN NOT READ FROM DATABASE , ");
	}

	public List read() {
		int retry = 0;
		while (retry < MAX_RETRY) {
			try {
				return readNextPage(cursorTimstamp, PageRequest.of(0, bulkSize + 1));
			} catch (Exception e) {
				retry++;
				try {
					Thread.sleep(2000);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
				log.error(e.getMessage());
			}
		}
		log.error("current status : " + this.toString());
		throw new RuntimeException("CAN NOT READ FROM DATABASE , ");
	}

	protected void printCurrentTimeStamp() {
		LocalDateTime time =
			LocalDateTime.ofInstant(Instant.ofEpochMilli(cursorTimstamp), ZoneId.systemDefault());
		log.info("long timestamp : {}", cursorTimstamp);
		log.info("of date : {}", time);
	}

	protected abstract void updateTimeStamp(List<PG_TYPE> rows);

	public boolean endOfTable(List list) {
		return list == null || list.size() == 0 || list.size() < this.bulkSize + 1;
	}

	public abstract ES_TYPE mapToDoc(PG_TYPE row);

	public abstract void upsertDoc(ES_TYPE doc);

	public abstract List<PG_TYPE> readNextPage(Long cursorTimstamp, Pageable pageable);

	public abstract void setPGRepo(COIN_CODE coinCode);

}
