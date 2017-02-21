package com.github.mte.run;


import com.github.mte.model.Config;
import com.github.mte.service.BondingService;
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
        bondingService.createScheme();
        taskRegistrar.addTriggerTask(bondingService::syncData, new CronTrigger(config.getCron()));
    }
}
