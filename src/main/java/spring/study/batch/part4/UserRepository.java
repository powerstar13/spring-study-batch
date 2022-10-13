package spring.study.batch.part4;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.List;

public interface UserRepository extends JpaRepository<User, Long> {
    
    List<User> findAllByUpdatedDate(LocalDate updatedDate);
    
    @Query(value = "SELECT MIN(u.id) FROM users u")
    long findMinId();
    
    @Query(value = "SELECT MAX(u.id) FROM users u")
    long findMaxId();
}
