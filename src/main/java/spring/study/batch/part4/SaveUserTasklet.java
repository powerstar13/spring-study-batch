package spring.study.batch.part4;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import spring.study.batch.part5.Orders;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
public class SaveUserTasklet implements Tasklet {
    
    private final int SIZE = 10_000;
    
    private final UserRepository userRepository;
    
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    
        List<User> users = this.createUsers();
    
        Collections.shuffle(users); // user를 섞는다.
    
        userRepository.saveAll(users);
        
        return RepeatStatus.FINISHED;
    }
    
    private List<User> createUsers() {
    
        List<User> users = new ArrayList<>();
    
        for (int i = 0; i < SIZE; i++) {
            // NORMAL
            users.add(
                User.builder()
                    .orders(Collections.singletonList(
                        Orders.builder()
                            .itemName("item" + i)
                            .amount(1_000)
                            .createdDate(LocalDate.of(2020, 11, 1))
                            .build()
                    ))
                    .username("test username" + i)
                    .build()
            );
        }
    
        for (int i = 0; i < SIZE; i++) {
            // SILVER 등급
            users.add(
                User.builder()
                    .orders(Collections.singletonList(
                        Orders.builder()
                            .itemName("item" + i)
                            .amount(200_000)
                            .createdDate(LocalDate.of(2020, 11, 2))
                            .build()
                    ))
                    .username("test username" + i)
                    .build()
            );
        }
    
        for (int i = 0; i < SIZE; i++) {
            // GOLD 등급
            users.add(
                User.builder()
                    .orders(Collections.singletonList(
                        Orders.builder()
                            .itemName("item" + i)
                            .amount(300_000)
                            .createdDate(LocalDate.of(2020, 11, 3))
                            .build()
                    ))
                    .username("test username" + i)
                    .build()
            );
        }
    
        for (int i = 0; i < SIZE; i++) {
            // VIP 등급
            users.add(
                User.builder()
                    .orders(Collections.singletonList(
                        Orders.builder()
                            .itemName("item" + i)
                            .amount(500_000)
                            .createdDate(LocalDate.of(2020, 11, 4))
                            .build()
                    ))
                    .username("test username" + i)
                    .build()
            );
        }
        
        return users;
    }
}
