package spring.study.batch.part4;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class UserConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    
    @Bean
    public Job userJob() {
        
        return this.jobBuilderFactory.get("userJob")
            .incrementer(new RunIdIncrementer())
            .start(this.saveUseStep())
            .build();
    }
    
    @Bean
    public Step saveUseStep() {
        
        return this.stepBuilderFactory.get("saveUseStep")
            .tasklet(new SaveUserTasklet(userRepository))
            .build();
    }
}
