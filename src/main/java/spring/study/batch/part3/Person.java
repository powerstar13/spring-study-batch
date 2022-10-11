package spring.study.batch.part3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Person {

    private int id;
    private String name;
    private String age;
    private String address;
}
