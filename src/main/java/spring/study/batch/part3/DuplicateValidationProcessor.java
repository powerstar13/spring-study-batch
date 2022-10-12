package spring.study.batch.part3;

import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DuplicateValidationProcessor<T> implements ItemProcessor<T, T> {
    
    private final Map<String, Object> keyPool = new ConcurrentHashMap<>(); // 중복 체크를 할 수 있는 map
    private final Function<T, String> keyExtractor; // I/O를 의미하는 제네릭 T 타입을 받아서 key를 추출한다.
    private final boolean allowDuplicate; // 필터링 여부를 설정하는 값 (true: 필터링을 하지 않겠다.)
    
    public DuplicateValidationProcessor(Function<T, String> keyExtractor, boolean allowDuplicate) {
        this.keyExtractor = keyExtractor;
        this.allowDuplicate = allowDuplicate;
    }
    
    @Override
    public T process(T item) throws Exception {
        
        if (allowDuplicate) return item;
        
        String key = keyExtractor.apply(item); // item을 받아서 key를 추출한다.
        
        if (keyPool.containsKey(key)) return null; // key가 keyPool에 이미 존재할 경우
        
        // keyPool에 key가 존재하지 않을 경우 keyPool에 보관하여 다음 key의 중복 여부를 검사하도록 준비
        keyPool.put(key, key);
        
        return item;
    }
}
