package spring.study.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing // Batch를 작동하기 위해 명시
@SpringBootApplication
public class SpringStudyBatchApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(SpringStudyBatchApplication.class, args);
    }
    
}
