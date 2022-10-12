package spring.study.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SavePersonConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    
    @Bean
    public Job savePersonJob() throws Exception {
        
        return this.jobBuilderFactory.get("savePersonJob")
            .incrementer(new RunIdIncrementer())
            .start(this.savePersonStep(null))
            .listener(new SavePersonListener.SavePersonJobExecutionListener())
            .listener(new SavePersonListener.SavePersonAnnotationJobExecutionListener())
            .build();
    }
    
    @Bean
    @JobScope // Spring Batch의 jobParameters를 Step에서 사용하기 위해 @JobScope을 설정해야 한다. (jobParameters를 SpEL로 사용하기 위해 @JobScope이 항상 필요하다.)
    public Step savePersonStep(@Value("#{jobParameters[allow_duplicate]}") String allowDuplicate) throws Exception {
        
        return this.stepBuilderFactory.get("savePersonStep")
            .<Person, Person>chunk(10)
            .reader(this.itemReader())
            .processor(this.itemProcessor(allowDuplicate)) // allowDuplicate가 null인 경우 기본적으로 false로 전달된다.
            .writer(this.itemWriter())
            .listener(new SavePersonListener.SavePersonStepExecutionListener())
            .faultTolerant() // faultTolerant() 메서드 뒤에 SkipListener가 와야 한다.
            .skip(NotFoundNameException.class)
            .skipLimit(2)
            .build();
    }
    
    private ItemProcessor<? super Person, ? extends Person> itemProcessor(String allowDuplicate) throws Exception {
    
        DuplicateValidationProcessor<Person> duplicateValidationProcessor = new DuplicateValidationProcessor<>(Person::getName, Boolean.parseBoolean(allowDuplicate));
    
        ItemProcessor<Person, Person> validationProcessor = item -> {
            
            if (item.isNotEmptyName()) return item;
    
            throw new NotFoundNameException();
        };
    
        CompositeItemProcessor<Person, Person> itemProcessor = new CompositeItemProcessorBuilder<Person, Person>()
            .delegates(new PersonValidationRetryProcessor(), validationProcessor, duplicateValidationProcessor) // PersonValidationRetryProcessor에 의해 Person.name이 모두 존재한 상태로 넘어가기 때문에 validationProcessor는 무의미하게 된다.
            .build();
        itemProcessor.afterPropertiesSet();
    
        return itemProcessor;
    }
    
    private ItemWriter<Person> itemWriter() throws Exception {
        
//        return items -> items.forEach(person -> log.info("저는 {}입니다.", person.getName()));
    
        JpaItemWriter<Person> jpaItemWriter = new JpaItemWriterBuilder<Person>()
            .entityManagerFactory(entityManagerFactory)
            .build();
        
        ItemWriter<Person> logItemWriter = items -> log.info("person.size: {}", items.size());
    
        // WARN: CompositeItemWriter를 사용할 때에는 ItemWriter가 중복으로 처리될 수도 있고 순서가 중요하기 때문에 사용에 주의해야 함
        CompositeItemWriter<Person> itemWriter = new CompositeItemWriterBuilder<Person>()
            .delegates(jpaItemWriter, logItemWriter) // 먼저 실행될 jpaItemWriter, 다음으로 실행될 logItemWriter
            .build();
        itemWriter.afterPropertiesSet();
    
        return itemWriter;
    }
    
    private ItemReader<Person> itemReader() throws Exception {
    
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("name", "age", "address");
        
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSet ->
            Person.builder()
                .name(fieldSet.readString(0))
                .age(fieldSet.readString(1))
                .address(fieldSet.readString(2))
                .build()
        );
    
        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
            .name("savePersonItemReader")
            .encoding("UTF-8")
            .linesToSkip(1)
            .resource(new ClassPathResource("person.csv"))
            .lineMapper(lineMapper)
            .build();
        itemReader.afterPropertiesSet();
    
        return itemReader;
    }
}
