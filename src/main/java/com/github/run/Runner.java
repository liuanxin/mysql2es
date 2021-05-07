package com.github.run;

import com.github.service.SyncDataService;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class Runner implements ApplicationRunner {

    private final SyncDataService syncDataService;

    @Override
    public void run(ApplicationArguments args) {
        syncDataService.init();
    }
}
