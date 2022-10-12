package spring.study.batch.part4;

import lombok.*;
import spring.study.batch.part5.Orders;

import javax.persistence.*;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

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
    
    @OneToMany(cascade = CascadeType.PERSIST)
    @JoinColumn(name = "user_id")
    private List<Orders> orders;
    
    private LocalDate updatedDate;
    
    @Builder
    public User(String username, List<Orders> orders) {
        this.username = username;
        this.orders = orders;
    }
    
    public boolean availableLevelUp() {
        
        return Level.availableLevelUp(this.getLevel(), this.getTotalAmount());
    }
    
    private int getTotalAmount() {
        
        return orders.stream()
            .mapToInt(Orders::getAmount)
            .sum();
    }
    
    public Level levelUp() {
    
        Level nextLevel = Level.getNextLevel(this.getTotalAmount());
    
        this.level = nextLevel;
        this.updatedDate = LocalDate.now();
    
        return nextLevel;
    }
    
    @AllArgsConstructor
    public enum Level {
        VIP(500_000, null),
        GOLD(500_000, VIP),
        SILVER(300_000, GOLD),
        NORMAL(200_000, SILVER);
        
        private final int nextAmount;
        private final Level nextLevel;
    
        private static boolean availableLevelUp(Level level, int totalAmount) {
            
            if (Objects.isNull(level)) return false;
            
            if (Objects.isNull(level.nextLevel)) return false; // VIP에 해당하는 경우 등급 상향할 level이 없음
            
            return totalAmount >= level.nextAmount;
        }
    
        private static Level getNextLevel(int totalAmount) {
            
            if (totalAmount >= Level.VIP.nextAmount) return VIP;
            if (totalAmount >= Level.GOLD.nextAmount) return GOLD.nextLevel; // VIP로 상향
            if (totalAmount >= Level.SILVER.nextAmount) return SILVER.nextLevel; // GOLD로 상향
            if (totalAmount >= Level.NORMAL.nextAmount) return NORMAL.nextLevel; // SILVER로 상향
            
            return NORMAL;
        }
    }
}
