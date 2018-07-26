package com.github.run;

import com.github.service.BondingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("!test")
@Configuration
public class Runner implements ApplicationRunner {

    @Autowired
    private BondingService bondingService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        bondingService.createScheme();
    }
}
