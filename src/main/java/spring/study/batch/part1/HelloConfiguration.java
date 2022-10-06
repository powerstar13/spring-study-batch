package spring.study.batch.part1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class HelloConfiguration {
    
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    
    @Bean
    public Job helloJob() { // Job: Sprinb Batch의 실행 단위
    
        return jobBuilderFactory.get("helloJob") // get() 메서드의 값으로 Job의 이름을 넣을 수 있다.
            .incrementer(new RunIdIncrementer()) // 실행 단위를 구분할 수 있는 incrementer (RunIdIncrementer는 Job이 실행될 때마다 ID가 자동으로 생성되도록 해준다.)
            .start(this.helloStep()) // 최초로 실행될 start() 메서드
            .build();
    }
    
    @Bean
    public Step helloStep() { // Step: Job의 실행 단위 (1개의 Job은 1개 이상의 Step을 가질 수 있다.)
    
        return stepBuilderFactory.get("helloStep")
            .tasklet((contribution, chunkContext) -> { // tasklet: 실행 단위
            
                log.info("hello spring batch");
                
                return RepeatStatus.FINISHED;
            })
            .build();
    }
}
