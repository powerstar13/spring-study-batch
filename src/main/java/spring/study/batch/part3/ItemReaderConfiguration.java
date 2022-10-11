package spring.study.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemReaderConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    
    @Bean
    public Job itemReaderJob() {
        
        return this.jobBuilderFactory.get("itemReaderJob")
            .incrementer(new RunIdIncrementer())
            .start(this.customItemReaderStep())
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
