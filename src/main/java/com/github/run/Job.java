package com.github.run;

import com.github.model.Config;
import com.github.service.BondingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

@Profile("!test")
@Configuration
public class Job implements SchedulingConfigurer {

    @Autowired
    private Config config;

    @Autowired
    private BondingService bondingService;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        // final boolean flag = bondingService.createScheme();
        taskRegistrar.addTriggerTask(new Runnable() {
            @Override
            public void run() {
                // if (flag) {
                    bondingService.syncData();
                // }
            }
        }, new CronTrigger(config.getCron()));
    }
}
