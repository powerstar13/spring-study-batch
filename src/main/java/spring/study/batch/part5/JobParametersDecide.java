package spring.study.batch.part5;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

@RequiredArgsConstructor
public class JobParametersDecide implements JobExecutionDecider {
    
    public static final FlowExecutionStatus CONTINUE = new FlowExecutionStatus("CONTINUE"); // 상태를 커스텀 정의
    private final String key; // JobParameters의 key이다. 해당 key에 값이 있는지 없는지 확인하기 위한 변수
    
    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
    
        String value = jobExecution.getJobParameters().getString(key);
        
        if (StringUtils.isEmpty(value)) return FlowExecutionStatus.COMPLETED;
    
        return CONTINUE;
    }
}
