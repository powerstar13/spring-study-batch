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
