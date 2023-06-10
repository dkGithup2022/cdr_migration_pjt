
## 마이그레이션 앱 


### 앱 목적
1. 현재 pgsql 에 저장된 bithumb, upbit 가격정보 데이터를 elasticsearch 로 이전합니다.
2. orderbook 의 경우 현재 모든 변경분을 저장하고 있습니다. 이걸 es 에 옮길때는 1분에 1개 단위로 샘플링을 하도록 합니다 .

</br>

---

### 진행 상황 & 이슈
 cdr_elasticsearch 에 추가된 bulk operation 의 테스트 코드에 이슈가 있습니다.

es bulk 결과가 es 에 바로 조회가 되지 않으면서 테스트가 깨지는 부분이 있어서 임시로 막아놓은 상황입니다.
해당 테스트는 module pom 파일의 <BulkOperationTestOnModule> 가 false 이면 실행되지 않으니, 테스트 깨지면 해당 밸류가 false 로 되어 있는지 확인 


</br>

###### 기록: 실행 시 인텔리제이에서 실행할 것 추천.

* 6월 추가 이슈 :  bulk가 아닌 parametized test 반복에서도 비슷한 현상이 발견됨. 해당 문제는 테스트를 다시 돌리거나 jar 로 만들지 말고 인텔리제이에서 그냥 실행 버튼 누르는 것으로도 해결됨.



* 현재로서 해당 문제를 해결하는 가장 쉬운 방법은 읽기 세그먼트에 정보가 반영될 때까지 긴  sleep을 주는 것이지만 이걸 하느니 그냥 mvn test 없이 실행하는것이 낫다고 봄.



</br>

-----


### Migration

해당 코드의 /migration/AbstractMigration 에 공통 작업 내용에 대한 슈퍼 클래스가 있습니다. 


##### super class 


```agsl


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
	protected Integer bulkSize = 1000;
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
		this.cursorTimstamp = Long.MAX_VALUE;
	}

	public void migrate(COIN_CODE coinCode) {

		setPGRepo(coinCode);
		log.info("\n\n******************************************************");
		log.info("BEGIN MIGRATION JOB ON {}", coinCode.toString());
		migratedCnt = 0L;
		Long iter = 0L;
		while (true) {
			// read from postgres
			List<PG_TYPE> rows = read();
			List<ES_TYPE> docs = rows.stream().map(e -> mapToDoc(e)).collect(Collectors.toList());

			//update data
			bulkFetch(docs);

			// update total cnt
			migratedCnt += docs.size();

			// update timestamp
			updateTimeStamp(rows);

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

				return;
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
		throw new RuntimeException("CAN NOT READ UPSERT , ");
	}

	public void bulkFetch(List<ES_TYPE> docs) {
		int retry = 0;
		while (retry < MAX_RETRY) {
			try {
				bulkInsert(docs);
				return;
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
		throw new RuntimeException("CAN NOT READ UPSERT , ");
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

	protected abstract void bulkInsert(List<ES_TYPE> docs) throws JsonProcessingException;

	public boolean endOfTable(List list) {
		return list == null || list.size() == 0 || list.size() < this.bulkSize + 1;
	}

	public abstract ES_TYPE mapToDoc(PG_TYPE row);

	public abstract void upsertDoc(ES_TYPE doc);

	public abstract List<PG_TYPE> readNextPage(Long cursorTimstamp, Pageable pageable);

	public abstract void setPGRepo(COIN_CODE coinCode);

}
```



</br>

##### migration 
아래는 모든 타입에 적용 될 migration 작업에 대한 코드 입니다.

1. postgresql dao 구현체 설정 
2. timestamp 기준으로 역순으로 읽기 시작함 .
3. elasticsearch 로 bulk operation 
4. 다음 read 를 위한 timestamp 업데이트
5. (2) 결과의 갯수 비교를 통해 더 읽을 데이터 있는지 확인
6. 2 ~ 5 를 2가 false 일 때 까지 반복 .

</br>

```
public void migrate(COIN_CODE coinCode) {

		setPGRepo(coinCode); // 1. postgresql dao 구현체 설정 
		log.info("\n\n******************************************************");
		log.info("BEGIN MIGRATION JOB ON {}", coinCode.toString());
		migratedCnt = 0L;
		Long iter = 0L;
		while (true) {
			// read from postgres
			List<PG_TYPE> rows = read(); // 2. timestamp 기준으로 역순으로 읽기 시작함 .
			List<ES_TYPE> docs = rows.stream().map(e -> mapToDoc(e)).collect(Collectors.toList());

			//update data
			bulkFetch(docs); // 3. elasticsearch 로 bulk operation 

			// update total cnt
			migratedCnt += docs.size();

			// update timestamp
			updateTimeStamp(rows); // 4.다음 read 를 위한 timestamp 업데이트

			// check it is end;
			if (endOfTable(rows)) // 5. (2) 결과의 갯수 비교를 통해 더 읽을 데이터 있는지 확인
				break;


			iter++;
			if (iter % 1000 == 0)
				printCurrentTimeStamp();

		}

		log.info("\n\n******************************************************");
		log.info("MIGRATION JOB DONE ON TICK {}", coinCode.toString());
		log.info("TOTAL {} DATA FETCHED ", migratedCnt);
	}

```


</br>

##### 읽기 연산

읽기, 쓰기 연산은 exception 에 대해 3번까지 시도하고 실패시 로그로 남기는 연산이 공통적으로 적용됩니다.


- pgsql read 
```agsl
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

```


</br>

- elasticsearch bulk
```agsl

	public void bulkFetch(List<ES_TYPE> docs) {
		int retry = 0;
		while (retry < MAX_RETRY) {
			try {
				bulkInsert(docs);
				return;
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
		throw new RuntimeException("CAN NOT READ UPSERT , ");
	}

```


</br>

##### 맴버 변수

- ES_REPO esRepository;
- PG_REPO rdmsRepo; 

각 구현체 클래스에 사용될 es dao, pgsql dao 에 대한 타입을 제너릭으로 선언합니다.


- PG_TYPE
- ES_TYPE

위의 dao 구현체에서 다룰 타입도 제너릭으로 받습니다.

가령 UbitTick에 대한 migration class 를 제작할 때, 

- ES_REPO = UpbitTickRepository (elasticsearch dao)
- PG_REPO = UpbitTickRepository ( rdms dao)
- PG_TYPE = UpbitTick (rdms entity)
- ES_TYPE = UpbitTick (es document type)



- 구현체의 타입 정의 예시 
- 
```agsl
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

```




</br>
