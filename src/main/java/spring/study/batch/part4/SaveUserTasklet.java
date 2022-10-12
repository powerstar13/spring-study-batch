package spring.study.batch.part4;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
public class SaveUserTasklet implements Tasklet {
    
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
    
        for (int i = 0; i < 100; i++) {
    
            users.add(
                User.builder()
                    .totalAmount(1_000) // NORMAL
                    .username("test username" + i)
                    .build()
            );
        }
    
        for (int i = 100; i < 200; i++) {
            
            users.add(
                User.builder()
                    .totalAmount(200_000) // SILVER 등급
                    .username("test username" + i)
                    .build()
            );
        }
    
        for (int i = 200; i < 300; i++) {
            
            users.add(
                User.builder()
                    .totalAmount(300_000) // GOLD 등급
                    .username("test username" + i)
                    .build()
            );
        }
    
        for (int i = 300; i < 400; i++) {
            
            users.add(
                User.builder()
                    .totalAmount(500_000) // VIP 등급
                    .username("test username" + i)
                    .build()
            );
        }
        
        return users;
    }
}
