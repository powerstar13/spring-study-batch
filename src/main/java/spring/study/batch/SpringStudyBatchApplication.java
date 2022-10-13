package spring.study.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@EnableBatchProcessing // Batch를 작동하기 위해 명시
@SpringBootApplication
public class SpringStudyBatchApplication {
    
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(SpringStudyBatchApplication.class, args))); // Spring Batch가 정상적으로 종료될 수 있도록 exit() 메서드 추가 (Async로 실행하면 종료가 안될 때가 간헐적으로 발생하기 때문에 안전하게 종료할 수 있도록 처리)
    }
    
    @Bean
    @Primary // SpringBoot에서 기본적으로 TaskExecutor를 Bean으로 제공하고 있기 때문에 사용자 정의한 TaskExecutor를 Bean으로 사용하기 위해 @Primary 애노테이션 사용
    public TaskExecutor taskExecutor() {
    
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor(); // Pool 안에서 Thread를 미리 만들어놓고 필요할 때마다 꺼내다 쓸 수 있기 때문에 효율적이다.
        taskExecutor.setCorePoolSize(10); // pool의 기본 size
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setThreadNamePrefix("batch-thread-");
        taskExecutor.initialize();
    
        return taskExecutor;
    }
    
}
