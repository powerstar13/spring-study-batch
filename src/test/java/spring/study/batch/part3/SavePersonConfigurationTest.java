package spring.study.batch.part3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import spring.study.batch.TestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@ContextConfiguration(classes = { SavePersonConfiguration.class, TestConfiguration.class }) // Test 대상을 설정 및 Test에서만 사용할 Configuraiton 추가
@SpringBatchTest
@RunWith(SpringRunner.class)
class SavePersonConfigurationTest {
    
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private PersonRepository personRepository;
    
    @AfterEach
    void tearDown() {
        personRepository.deleteAll();
    }
    
    @Test
    void testStep() {
        
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("savePersonStep");
        
        assertThat(jobExecution.getStepExecutions() // N개의 Step을 가져온다.
            .stream()
            .mapToInt(StepExecution::getWriteCount) // Write된 개수를 가져온다.
            .sum())
            .isEqualTo(personRepository.count())
            .isEqualTo(3);
    }
    
    @Test
    void testAllowDuplicate() throws Exception {
        // given
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("allow_duplicate", "false")
            .toJobParameters();
        
        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        
        // then
        assertThat(jobExecution.getStepExecutions() // N개의 Step을 가져온다.
            .stream()
            .mapToInt(StepExecution::getWriteCount) // Write된 개수를 가져온다.
            .sum())
            .isEqualTo(personRepository.count())
            .isEqualTo(3);
    }
    
    @Test
    void testNotAllowDuplicate() throws Exception {
        // given
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("allow_duplicate", "true")
            .toJobParameters();
        
        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        
        // then
        assertThat(jobExecution.getStepExecutions() // N개의 Step을 가져온다.
            .stream()
            .mapToInt(StepExecution::getWriteCount) // Write된 개수를 가져온다.
            .sum())
            .isEqualTo(personRepository.count())
            .isEqualTo(100);
    }
}