package spring.study.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemWriterConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;
    
    @Bean
    public Job itemWriterJob() throws Exception {
        
        return this.jobBuilderFactory.get("itemWriterJob")
            .incrementer(new RunIdIncrementer())
            .start(this.csvItemWriterStep())
            .next(this.jdbcBatchItemWriterStep())
            .build();
    }
    
    @Bean
    public Step csvItemWriterStep() throws Exception {
        
        return this.stepBuilderFactory.get("csvItemWriterStep")
            .<Person, Person>chunk(10)
            .reader(this.itemReader())
            .writer(this.csvFileItemWriter())
            .build();
    }
    
    @Bean
    public Step jdbcBatchItemWriterStep() {
    
        return stepBuilderFactory.get("jdbcBatchItemWriterStep")
            .<Person, Person>chunk(10)
            .reader(this.itemReader())
            .writer(this.jdbcBatchItemWriter())
            .build();
    }
    
    private ItemWriter<Person> jdbcBatchItemWriter() {
    
        JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriterBuilder<Person>()
            .dataSource(dataSource)
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO person (name, age, address) VALUES (:name, :age, :address)")
            .build();
        itemWriter.afterPropertiesSet();
    
        return itemWriter;
    }
    
    private ItemWriter<Person> csvFileItemWriter() throws Exception {
    
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] { "id", "name", "age", "address" });
    
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);
    
        FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriterBuilder<Person>()
            .name("csvFileItemWriter")
            .encoding("UTF-8")
            .resource(new FileSystemResource("output/test-output.csv"))
            .lineAggregator(lineAggregator)
            .headerCallback(writer -> writer.write("id,이름,나이,거주지"))
            .footerCallback(writer -> writer.write("----------------\n"))
            .append(true) // true: 덮어쓰기가 아닌 이어쓰기
            .build();
        itemWriter.afterPropertiesSet();
    
        return itemWriter;
    }
    
    private ItemReader<Person> itemReader() {
        
        return new CustomItemReader<>(this.getItems());
    }
    
    private List<Person> getItems() {
        
        List<Person> items = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
            
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
