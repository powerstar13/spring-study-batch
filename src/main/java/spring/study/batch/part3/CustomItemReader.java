package spring.study.batch.part3;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.ArrayList;
import java.util.List;

public class CustomItemReader<T> implements ItemReader<T> {
    
    private final List<T> items;
    
    public CustomItemReader(List<T> items) {
        this.items = new ArrayList<>(items); // 새로운 ArrayList로 들어갈 수 있도록 다시 담는다.
    }
    
    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        
        if (!items.isEmpty()) {
            return items.remove(0); // 첫 번째 인덱스의 item을 반환하고 제거한다.
        }
        
        return null; // null을 반환하면 chunk 반복의 끝을 알린다는 의미이다.
    }
}
