CREATE TABLE person (
    id      BIGINT PRIMARY KEY AUTO_INCREMENT,
    name    VARCHAR(255),
    age     VARCHAR(255),
    address VARCHAR(255)
);

INSERT INTO person (name, age, address)
VALUES ('이순신','32','인천');
INSERT INTO person (name, age, address)
VALUES ('홍길동','30','서울');
INSERT INTO person (name, age, address)
VALUES ('아무개','25','강원');
