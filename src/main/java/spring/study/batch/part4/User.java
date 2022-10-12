package spring.study.batch.part4;

import lombok.*;

import javax.persistence.*;
import java.time.LocalDate;

@Entity(name = "users")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class User {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    
    private String username;
    
    @Enumerated(EnumType.STRING)
    private Level level = Level.NORMAL;
    
    private int totalAmount;
    
    private LocalDate updatedDate;
    
    @Builder
    public User(String username, int totalAmount) {
        this.username = username;
        this.totalAmount = totalAmount;
    }
    
    @AllArgsConstructor
    public enum Level {
        VIP(500_000, null),
        GOLD(500_000, VIP),
        SILVER(300_000, GOLD),
        NORMAL(200_000, SILVER);
        
        private final int nextAmount;
        private final Level nextLevel;
    }
}
