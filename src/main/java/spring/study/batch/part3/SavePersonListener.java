package spring.study.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeStep;

@Slf4j
public class SavePersonListener {
    
    public static class SavePersonStepExecutionListener {
        
        @BeforeStep
        public void beforeStep(StepExecution stepExecution) {
            log.info("beforeStep");
        }
        
        @AfterStep
        public ExitStatus afterStep(StepExecution stepExecution) {
            
            log.info("afterStep: {}", stepExecution.getWriteCount());
            
//            if (stepExecution.getWriteCount() == 0) return ExitStatus.FAILED; // 조건에 따라 status를 처리할 수도 있다.
    
            return stepExecution.getExitStatus(); // StepExecution이 종료되거나 실패한 상태를 그대로 반환한다.
        }
    }
    
    public static class SavePersonJobExecutionListener implements JobExecutionListener {
    
        @Override
        public void beforeJob(JobExecution jobExecution) {
            log.info("beforeJob");
        }
    
        @Override
        public void afterJob(JobExecution jobExecution) {
    
            int sum = jobExecution.getStepExecutions().stream()
                .mapToInt(StepExecution::getWriteCount)
                .sum();
            
            log.info("afterJob: {}", sum);
        }
    }
    
    public static class SavePersonAnnotationJobExecutionListener {
    
        @BeforeJob
        public void beforeJob(JobExecution jobExecution) {
            log.info("annotationBeforeJob");
        }
    
        @AfterJob
        public void afterJob(JobExecution jobExecution) {
        
            int sum = jobExecution.getStepExecutions().stream()
                .mapToInt(StepExecution::getWriteCount)
                .sum();
        
            log.info("annotationAfterJob: {}", sum);
        }
    }
}
