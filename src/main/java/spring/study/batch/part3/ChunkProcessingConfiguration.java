package spring.study.batch.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
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
            .next(this.chunkBaseStep())
            .build();
    }

    @Bean
    public Step chunkBaseStep() {

        return stepBuilderFactory.get("chunkBaseStep")
            .<String, String>chunk(10) // 100개의 데이터가 있을 경우 10개씩 데이터를 나누어 처리하라는 뜻이다.
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
            .tasklet(this.tasklet())
            .build();
    }

    private Tasklet tasklet() {

        List<String> items = this.getItems();

        return (contribution, chunkContext) -> {

            StepExecution stepExecution = contribution.getStepExecution();

            int chunkSize = 10;
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
