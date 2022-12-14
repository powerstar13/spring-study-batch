package spring.study.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemReaderConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;
    private final EntityManagerFactory entityManagerFactory;
    
    @Bean
    public Job itemReaderJob() throws Exception {
        
        return this.jobBuilderFactory.get("itemReaderJob")
            .incrementer(new RunIdIncrementer())
            .start(this.customItemReaderStep())
            .next(this.csvFileStep())
            .next(this.jdbcStep())
            .next(this.jpaStep())
            .build();
    }
    
    @Bean
    public Step customItemReaderStep() {
        
        return this.stepBuilderFactory.get("customItemReaderStep")
            .<Person, Person>chunk(10)
            .reader(new CustomItemReader<>(this.getItems()))
            .writer(this.itemWriter())
            .build();
    }
    
    @Bean
    public Step csvFileStep() throws Exception {
    
        return stepBuilderFactory.get("csvFileStep")
            .<Person, Person>chunk(10)
            .reader(this.csvFileItemReader())
            .writer(this.itemWriter())
            .build();
    }
    
    @Bean
    public Step jdbcStep() throws Exception {
    
        return stepBuilderFactory.get("jdbcStep")
            .<Person, Person>chunk(10)
            .reader(this.jdbcCursorItemReader())
            .writer(this.itemWriter())
            .build();
    }
    
    @Bean
    public Step jpaStep() throws Exception {
    
        return stepBuilderFactory.get("jpaStep")
            .<Person, Person>chunk(10)
            .reader(this.jpaCursorItemReader())
            .writer(this.itemWriter())
            .build();
    }
    
    private JpaCursorItemReader<Person> jpaCursorItemReader() throws Exception {
    
        JpaCursorItemReader<Person> itemReader = new JpaCursorItemReaderBuilder<Person>()
            .name("jpaCursorItemReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString("SELECT p FROM Person p") // JPQL Query
            .build();
        itemReader.afterPropertiesSet();
        
        return itemReader;
    }
    
    private JdbcCursorItemReader<Person> jdbcCursorItemReader() throws Exception {
    
        JdbcCursorItemReader<Person> itemReader = new JdbcCursorItemReaderBuilder<Person>()
            .name("jdbcCursorItemReader")
            .dataSource(dataSource)
            .sql("SELECT id, name, age, address, FROM person")
            .rowMapper((rs, rowNum) ->
                Person.builder()
                    .id(rs.getInt(1)) // Column 인덱스는 0이 아닌 1부터 시작한다.
                    .name(rs.getString(2))
                    .age(rs.getString(3))
                    .address(rs.getString(4))
                    .build()
            )
            .build();
        itemReader.afterPropertiesSet();
    
        return itemReader;
    }
    
    private FlatFileItemReader<Person> csvFileItemReader() throws Exception {
    
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>(); // csv 파일을 한 줄씩 읽을 수 있는 LineMapper 객체
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "address"); // Person의 필드들을 등록해준다.
        lineMapper.setLineTokenizer(tokenizer);
        
        lineMapper.setFieldSetMapper(fieldSet -> {
    
            int id = fieldSet.readInt("id");
            String name = fieldSet.readString("name");
            String age = fieldSet.readString("age");
            String address = fieldSet.readString("address");
            
            return new Person(id, name, age, address);
        });
    
        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
            .name("csvFileItemReader")
            .encoding("UTF-8")
            .resource(new ClassPathResource("test.csv"))
            .linesToSkip(1) // csv 파일의 첫 번째 줄인 id,이름,나이,거주지를 생략하도록 처리
            .lineMapper(lineMapper)
            .build();
        itemReader.afterPropertiesSet(); // item이 필요한 설정값이 잘 됐는지 검증하는 메서드
    
        return itemReader;
    }
    
    private ItemWriter<Person> itemWriter() {
        
        return items -> {
            log.info(
                items.stream()
                    .map(Person::getName)
                    .collect(Collectors.joining(", "))
            );
        };
    }
    
    private List<Person> getItems() {
    
        List<Person> items = new ArrayList<>();
    
        for (int i = 0; i < 10; i++) {
    
            items.add(
                Person.builder()
                    .id(i + 1)
                    .name("test name" + i)
                    .age("test age")
                    .address("test address")
                    .build()
            );
        }
        
        return items;
    }
}
