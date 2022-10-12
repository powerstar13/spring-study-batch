package spring.study.batch.part4;

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
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class UserConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    
    @Bean
    public Job userJob() throws Exception {
        
        return jobBuilderFactory.get("userJob")
            .incrementer(new RunIdIncrementer())
            .start(this.saveUseStep())
            .next(this.userLevelUpStep())
            .build();
    }
    
    @Bean
    public Step saveUseStep() {
        
        return stepBuilderFactory.get("saveUseStep")
            .tasklet(new SaveUserTasklet(userRepository))
            .build();
    }
    
    @Bean
    public Step userLevelUpStep() throws Exception {
    
        return stepBuilderFactory.get("userLevelUpStep")
            .<User, User>chunk(100)
            .reader(this.itemReader())
            .processor(this.itemProcessor())
            .writer(this.itemWriter())
            .build();
    }
    
    private ItemWriter<? super User> itemWriter() {
        
        return users -> users.forEach(user -> {
            user.levelUp();
            userRepository.save(user);
        });
    }
    
    private ItemProcessor<? super User,? extends User> itemProcessor() {
    
        return user -> {
            // 등급 상향 대상 유저의 경우
            if (user.availableLevelUp()) return user;
        
            return null;
        };
    }
    
    private ItemReader<? extends User> itemReader() throws Exception {
    
        JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
            .queryString("SELECT u FROM users u")
            .entityManagerFactory(entityManagerFactory)
            .pageSize(100) // pageSize는 보통 chunk size와 동일하게 설정한다.
            .name("userItemReader")
            .build();
        itemReader.afterPropertiesSet();
    
        return itemReader;
    }
}
