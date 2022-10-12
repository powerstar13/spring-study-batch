package spring.study.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

@Slf4j
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {
    
    private final RetryTemplate retryTemplate;
    
    public PersonValidationRetryProcessor() {
        
        this.retryTemplate = new RetryTemplateBuilder()
            .maxAttempts(3)
            .retryOn(NotFoundNameException.class)
            .withListener(new SavePersonRetryListener())
            .build();
    }
    
    @Override
    public Person process(Person item) throws Exception {
        
        // maxAttempts에서 지정한 횟수만큼 RetryCallback이 실행되고 다음부터는 RecoveryCallbak이 실행된다.
        return this.retryTemplate.execute(context -> {
            // RetryCallback
            if (item.isNotEmptyName()) return item;
    
            throw new NotFoundNameException();
            
        }, context -> {
            // RecoveryCallback
            return item.unknownName();
        });
    }
    
    public static class SavePersonRetryListener implements RetryListener {
    
        @Override
        public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
            return true; // true여야 retry가 적용된다.
        }
    
        @Override
        public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            // retry 종료 후에 호출된다.
            log.info("close");
        }
    
        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("onError");
        }
    }
}
