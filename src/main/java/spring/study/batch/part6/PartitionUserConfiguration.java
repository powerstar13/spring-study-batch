package spring.study.batch.part6;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import spring.study.batch.part4.LevelUpJobExecutionListener;
import spring.study.batch.part4.SaveUserTasklet;
import spring.study.batch.part4.User;
import spring.study.batch.part4.UserRepository;
import spring.study.batch.part5.JobParametersDecide;
import spring.study.batch.part5.OrderStatistics;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PartitionUserConfiguration {
    
    private final String JOB_NAME = "partitionUserJob";
    private final int CHUNK = 1_000;
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    private final TaskExecutor taskExecutor;
    
    @Bean(JOB_NAME)
    public Job userJob() throws Exception {
        
        return jobBuilderFactory.get(JOB_NAME)
            .incrementer(new RunIdIncrementer())
            .start(this.saveUseStep())
            .next(this.userLevelUpManagerStep()) // Master Step으로 설정
            .listener(new LevelUpJobExecutionListener(userRepository))
            .next(new JobParametersDecide("date")) // JobParameters로 date값에 따라 status가 결정됨
            .on(JobParametersDecide.CONTINUE.getName()) // status값이 CONTINUE인 경우에만 to() 메서드가 작동됨
            .to(this.orderStatisticsStep(null))
            .build()
            .build();
    }
    
    @Bean(JOB_NAME + "_orderStatisticsStep")
    @JobScope
    public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
    
        return stepBuilderFactory.get(JOB_NAME + "_orderStatisticsStep")
            .<OrderStatistics, OrderStatistics>chunk(CHUNK)
            .reader(this.orderStatisticsItemReader(date))
            .writer(this.orderStatisticsItemWriter(date))
            .build();
            
    }
    
    private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
    
        YearMonth yearMonth = YearMonth.parse(date);
    
        String fileName = String.format("%d년_%d월_일별_주문_금액.csv",
            yearMonth.getYear(),
            yearMonth.getMonthValue()
        );
    
        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] { "amount", "date" });
    
        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(","); // csv 파일이기 때문에 콤마(,)를 기준으로 설정
        lineAggregator.setFieldExtractor(fieldExtractor);
    
        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
            .name(JOB_NAME + "_orderStatisticsItemWriter")
            .resource(new FileSystemResource("output/" + fileName))
            .encoding("UTF-8")
            .lineAggregator(lineAggregator)
            .headerCallback(writer -> writer.write("total_amount,date"))
            .build();
        itemWriter.afterPropertiesSet();
    
        return itemWriter;
    }
    
    private ItemReader<? extends OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
    
        YearMonth yearMonth = YearMonth.parse(date);
    
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("startDate", yearMonth.atDay(1));
        parameters.put("endDate", yearMonth.atEndOfMonth());
    
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("created_date", Order.ASCENDING);
    
        JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
            .name(JOB_NAME + "_orderStatisticsItemReader")
            .dataSource(dataSource)
            .rowMapper((rs, rowNum) ->
                OrderStatistics.builder()
                    .amount(rs.getString(1))
                    .date(LocalDate.parse(rs.getString(2), DateTimeFormatter.ISO_DATE))
                    .build()
            )
            .pageSize(CHUNK) // chunk size와 동일하게 설정
            .selectClause("SUM(amount), created_date")
            .fromClause("orders")
            .whereClause("created_date >= :startDate AND created_date <= :endDate")
            .groupClause("created_date")
            .parameterValues(parameters)
            .sortKeys(sortKey)
            .build();
        itemReader.afterPropertiesSet();
    
        return itemReader;
    }
    
    @Bean(JOB_NAME + "_saveUseStep")
    public Step saveUseStep() {
        
        return stepBuilderFactory.get(JOB_NAME + "_saveUseStep")
            .tasklet(new SaveUserTasklet(userRepository))
            .build();
    }
    
    @Bean(JOB_NAME + "_userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
    
        return stepBuilderFactory.get(JOB_NAME + "_userLevelUpStep")
            .<User, Future<User>>chunk(CHUNK)
            .reader(this.itemReader(null, null))
            .processor(this.itemProcessor())
            .writer(this.itemWriter())
            .build();
    }
    
    @Bean(JOB_NAME + "_userLevelUpStep.manager")
    public Step userLevelUpManagerStep() throws Exception {
    
        return stepBuilderFactory.get(JOB_NAME + "_userLevelUpStep.manager")
            .partitioner(JOB_NAME + "_userLevelUpStep", new UserLevelUpPartitioner(userRepository))
            .step(this.userLevelUpStep())
            .partitionHandler(this.taskExecutorPartitionHandler())
            .build();
    }
    
    @Bean(JOB_NAME + "_taskExecutorPartitionHandler")
    public PartitionHandler taskExecutorPartitionHandler() throws Exception {
    
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(this.userLevelUpStep());
        handler.setTaskExecutor(taskExecutor);
        handler.setGridSize(8);
        
        return handler;
    }
    
    private AsyncItemWriter<User> itemWriter() {
        
        ItemWriter<User> itemWriter = users -> users.forEach(user -> {
            user.levelUp();
            userRepository.save(user);
        });
        
        AsyncItemWriter<User> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(itemWriter);
        
        return asyncItemWriter;
    }
    
    private AsyncItemProcessor<User, User> itemProcessor() {
        
        ItemProcessor<User, User> itemProcessor = user -> {
            // 등급 상향 대상 유저의 경우
            if (user.availableLevelUp()) return user;
            
            return null;
        };
        
        AsyncItemProcessor<User, User> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(itemProcessor); // ItemProcessor를 Delegate로 감싼다.
        asyncItemProcessor.setTaskExecutor(taskExecutor);
        
        return asyncItemProcessor;
    }
    
    @Bean // StepScope를 사용하기 위해 Bean으로 등록돼야 한다.
    @StepScope // ItemReader에서 ExecutionContext를 사용하기 위해 StepScope가 필요하다.
    public JpaPagingItemReader<? extends User> itemReader(
        @Value("#{stepExecutionContext[minId]}") Long minId,
        @Value("#{stepExecutionContext[maxId]}") Long maxId
    ) throws Exception {
    
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("minId", minId);
        parameters.put("maxId", maxId);
    
        JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
            .queryString("SELECT u FROM users u WHERE u.id BETWEEN :minId AND :maxId")
            .parameterValues(parameters)
            .entityManagerFactory(entityManagerFactory)
            .pageSize(CHUNK) // pageSize는 보통 chunk size와 동일하게 설정한다.
            .name(JOB_NAME + "_userItemReader")
            .build();
        itemReader.afterPropertiesSet();
    
        return itemReader;
    }
}
