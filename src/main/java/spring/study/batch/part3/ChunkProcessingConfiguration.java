package spring.study.batch.part3;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ChunkProcessingConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job chunkProcessJob() {

        return jobBuilderFactory.get("chunkProcessJob")
            .incrementer(new RunIdIncrementer())
            .start(this.taskBaseStep())
            .next(this.chunkBaseStep(null)) // chunkBaseStep() 메서드의 경우 @JobScope에서 chunkSize를 가져다 쓸 것이기 때문에 null로 파라미터를 전달해도 된다.
            .build();
    }

    @Bean
    @JobScope // @Value 애노테이션을 사용하기 위해 사용
    public Step chunkBaseStep(@Value("#{jobParameters[chunkSize]}") String chunkSize) { // WARN: lombok이 아닌 spring에서 제공하는 @Value 애노테이션을 사용

        return stepBuilderFactory.get("chunkBaseStep")
            .<String, String>chunk(
                StringUtils.isNotEmpty(chunkSize) ?
                    Integer.parseInt(chunkSize)
                    : 10
            ) // 전달된 개수만큼 데이터를 나누어 처리하라는 뜻이다.
            .reader(this.itemReader())
            .processor(this.itemProcessor())
            .writer(this.itemWriter())
            .build();
    }

    private ItemWriter<? super String> itemWriter() {

        return items -> {

            log.info("chunk item size: {}", items.size());
//            items.forEach(log::info);
        };
    }

    private ItemProcessor<? super String, String> itemProcessor() {

        // WARN: null로 return될 경우 Writer로 넘어갈 수 없게 된다.
        return item -> item + ", String Batch";
    }

    private ItemReader<String> itemReader() {

        return new ListItemReader<>(this.getItems()); // Spring Batch에서 제공하는 ListItemReader는 List를 파라미터로 받을 수 있다.
    }

    @Bean
    public Step taskBaseStep() {

        return stepBuilderFactory.get("taskBaseStep")
            .tasklet(this.tasklet(null))
            .build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters[chunkSize]}") String chunkSizeStr) {

        List<String> items = this.getItems();

        return (contribution, chunkContext) -> {

            StepExecution stepExecution = contribution.getStepExecution();
//            JobParameters jobParameters = stepExecution.getJobParameters();
//
//            String chunkSizeStr = jobParameters.getString("chunkSize", "10");
            int chunkSize = StringUtils.isNotEmpty(chunkSizeStr) ? Integer.parseInt(chunkSizeStr) : 10;

            int fromIndex = stepExecution.getReadCount(); // 현재까지 읽은 개수를 가져온다.
            int toIndex = fromIndex + chunkSize;

            if (fromIndex >= items.size()) {
                return RepeatStatus.FINISHED; // item의 사이즈를 다 처리한 경우 종료
            }

            List<String> subList = items.subList(fromIndex, toIndex); // subList() 메서드를 사용하여 페이징 처리를 할 수 있다.

            log.info("task item size: {}", subList.size());

            stepExecution.setReadCount(toIndex); // 지금까지 읽은 개수를 반영한다.

            return RepeatStatus.CONTINUABLE; // 이 tasklet을 반복해서 처리하라는 의미의 CONMTINUABLE
        };
    }

    private List<String> getItems() {

        List<String> items = new ArrayList<>();

        for (int i = 0; i < 100; i++) {

            items.add(i + " Hello");
        }

        return items;
    }
}
