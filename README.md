# 스프링 배치

1. Spring Batch Job 구성
2. 데이터 공유 ExecutionContext
   - Job 내에서 공유할 수 있는 BATCH_JOB_EXECUTION_CONTEXT
   - 하나의 Step에서 공유할 수 있는 BATCH_STEP_EXECUTION_CONTEXT
3. Task 기반 배치와 Chunk 기반 배치
4. JobParameters 사용
5. @JobScope와 @StepScope 이해
6. ItemReader interface 구조
   - ItemReader 인터페이스의 구현체로 CustomItemReader 만들어 사용하기
7. CSV 파일 데이터 읽기
   - FlatFileItemReader 클래스로 파일에 저장된 데이터를 읽어 객체에 매핑
8. JDBC 데이터 읽기
   - Cursor 기반 조회
   - Paging 기반 조회
9. JPA 데이터 읽기
10. CSV 파일 데이터 쓰기
   - FlatFileItemWriter는 데이터가 매핑된 객체를 파일로 write
11. JDBC 데이터 쓰기
12. JPA 데이터 쓰기
13. ItemProcessor interface 구조 이해
14. CSV 파일 데이터를 읽어 H2 DB에 데이터 저장하는 배치 개발
    - ItemReader, ItemWriter 작성
    - ItemProcessor 작성
    - @JobScope 설정으로 jobParameters 사용하기
    - ItemWriter에서 JpaItemWriter 사용하고 log 출력
15. 테스트 코드 작성하기
16. JobExecutionListener, StepExecutionListener
    - JobExecutionListener 인터페이스를 구현하는 방법
    - 애노테이션으로 정의하는 방법
      - @BeforeJob, @AfterJob
      - @BeforeStep, @AfterStep
17. Skip 예외 처리
18. retry 예외 처리
    - RetryListener 인터페이스 구현체를 RetryTemplate 리스너에 등록
19. 회원 등급 프로젝트 
    - 회원 데이터 H2 DB에 저장하기
    - 회원 주문 금액에 따른 등급 적용 Step 개발
    - JobExecutionListener로 대상 회원 데이터 로그와 실행 시간 측정
20. 주문금액 집계 프로젝트
    - User와 Orders Entity의 매핑 관계 개발
    - 일별 주문 금액 집계 Step 개발
    - JobExecutionDecider로 주문 금액 집계 Step 실행 여부 결정
21. 성능 개선과 성능 비교
    - 성능 개선 계획
    - Async Step 적용
    - Multi-Thread Step 적용
    - Partition Step 적용