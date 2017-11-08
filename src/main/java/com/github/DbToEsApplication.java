package com.github;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class DbToEsApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(DbToEsApplication.class).web(false).run(args);
    }
}
