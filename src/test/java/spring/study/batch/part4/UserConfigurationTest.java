package spring.study.batch.part4;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import spring.study.batch.TestConfiguration;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SpringBatchTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { UserConfiguration.class, TestConfiguration.class })
class UserConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private UserRepository userRepository;

    @Test
    void test() throws Exception {

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        int size = userRepository.findAllByUpdatedDate(LocalDate.now()).size();

        assertThat(jobExecution.getStepExecutions().stream()
            .filter(x -> x.getStepName().equals("userLevelUpStep"))
            .mapToInt(StepExecution::getWriteCount)
            .sum())
            .isEqualTo(size)
            .isEqualTo(300);

        assertThat(userRepository.count())
            .isEqualTo(400);
    }
}