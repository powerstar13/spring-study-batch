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
    @JobScope // Spring Batch??? jobParameters??? Step?????? ???????????? ?????? @JobScope??? ???????????? ??????. (jobParameters??? SpEL??? ???????????? ?????? @JobScope??? ?????? ????????????.)
    public Step savePersonStep(@Value("#{jobParameters[allow_duplicate]}") String allowDuplicate) throws Exception {
        
        return this.stepBuilderFactory.get("savePersonStep")
            .<Person, Person>chunk(10)
            .reader(this.itemReader())
            .processor(this.itemProcessor(allowDuplicate)) // allowDuplicate??? null??? ?????? ??????????????? false??? ????????????.
            .writer(this.itemWriter())
            .listener(new SavePersonListener.SavePersonStepExecutionListener())
            .faultTolerant() // faultTolerant() ????????? ?????? SkipListener??? ?????? ??????.
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
            .delegates(new PersonValidationRetryProcessor(), validationProcessor, duplicateValidationProcessor) // PersonValidationRetryProcessor??? ?????? Person.name??? ?????? ????????? ????????? ???????????? ????????? validationProcessor??? ??????????????? ??????.
            .build();
        itemProcessor.afterPropertiesSet();
    
        return itemProcessor;
    }
    
    private ItemWriter<Person> itemWriter() throws Exception {
        
//        return items -> items.forEach(person -> log.info("?????? {}?????????.", person.getName()));
    
        JpaItemWriter<Person> jpaItemWriter = new JpaItemWriterBuilder<Person>()
            .entityManagerFactory(entityManagerFactory)
            .build();
        
        ItemWriter<Person> logItemWriter = items -> log.info("person.size: {}", items.size());
    
        // WARN: CompositeItemWriter??? ????????? ????????? ItemWriter??? ???????????? ????????? ?????? ?????? ????????? ???????????? ????????? ????????? ???????????? ???
        CompositeItemWriter<Person> itemWriter = new CompositeItemWriterBuilder<Person>()
            .delegates(jpaItemWriter, logItemWriter) // ?????? ????????? jpaItemWriter, ???????????? ????????? logItemWriter
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
