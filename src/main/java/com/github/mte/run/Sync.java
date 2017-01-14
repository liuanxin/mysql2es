package com.github.mte.run;

import com.github.mte.model.Config;
import com.github.mte.repository.DataRepository;
import com.github.mte.service.BondingService;
import com.github.mte.util.U;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("!test")
@Configuration
public class Sync implements CommandLineRunner {

    @Autowired
    private Config config;

    @Autowired
    private DataRepository dataRepository;
    @Autowired
    private BondingService bondingService;

    @Override
    public void run(String... args) throws Exception {
        U.assertNil(config, "no config with mysql and es");
        config.check();

        String cron = config.getCron();
        if (U.isBlank(cron)) {
            bondingService.deleteScheme();
            bondingService.createScheme();
            dataRepository.syncData(U.EMPTY);
        }
    }
}
