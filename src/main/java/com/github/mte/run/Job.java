package com.github.mte.run;


import com.github.mte.model.Config;
import com.github.mte.service.BondingService;
import com.github.mte.util.U;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

@Profile("!test")
@Configuration
@EnableScheduling
public class Job implements SchedulingConfigurer {

    @Autowired
    private Config config;

    @Autowired
    private BondingService bondingService;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        U.assertNil(config, "no config with mysql and es");
        config.check();

        String cron = config.getCron();
        if (U.isNotBlank(cron)) {
            bondingService.createScheme();
            taskRegistrar.addTriggerTask(bondingService::saveOrUpdateData, new CronTrigger(config.getCron()));
        }
    }
}
