package com.github.run;

import com.github.model.Config;
import com.github.model.Document;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.util.List;

@Profile("!test")
@Configuration
public class Job implements SchedulingConfigurer {

    @Autowired
    private Config config;

    @Autowired
    private EsRepository esRepository;

    @Autowired
    private DataRepository dataRepository;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(new Runnable() {
            @Override
            public void run() {
                List<Document> documents = dataRepository.incrementData();
                esRepository.saveDataToEs(documents);
            }
        }, new CronTrigger(config.getCron()));
    }
}
