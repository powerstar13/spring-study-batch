package spring.study.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ItemProcessorConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    
    @Bean
    public Job itemProcessorJob() {
        
        return this.jobBuilderFactory.get("itemProcessorJob")
            .incrementer(new RunIdIncrementer())
            .start(this.itemProcessorStep())
            .build();
    }
    
    @Bean
    public Step itemProcessorStep() {
        
        return this.stepBuilderFactory.get("itemProcessorStep")
            .<Person, Person>chunk(10)
            .reader(this.itemReader())
            .processor(this.itemProcessor())
            .writer(this.itemWriter())
            .build();
    }
    
    private ItemWriter<Person> itemWriter() {
        
        return items -> items.forEach(person -> log.info("PERSON.ID: {}", person.getId()));
    }
    
    private ItemProcessor<? super Person, ? extends Person> itemProcessor() {
        
        return item -> {
            
            if (item.getId() % 2 == 0) { // 짝수인 것만 filtering
                return item;
            }
            
            return null;
        };
    }
    
    private ItemReader<Person> itemReader() {
        
        return new CustomItemReader<>(this.getItems());
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
