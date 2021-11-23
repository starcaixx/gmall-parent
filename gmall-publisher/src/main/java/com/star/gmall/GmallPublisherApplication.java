package com.star.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * MapperScan 注解相当于指定某个包或者多个包为接口mapper，不需要每个mapper单独去使用@Mapper
 */
@SpringBootApplication
@MapperScan(basePackages = "com.star.gmall.mapper")
public class GmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisherApplication.class, args);
    }

}
