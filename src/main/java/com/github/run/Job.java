package com.github.run;

import com.github.model.Config;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.util.Logs;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Profile("!test")
@Configuration
public class Job implements SchedulingConfigurer {

    @Autowired
    private Config config;

    @Autowired
    private DataRepository dataRepository;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(new Runnable() {
            @Override
            public void run() {
                List<Future<Boolean>> resultList = Lists.newArrayList();
                for (Relation relation : config.getRelation()) {
                    resultList.add(dataRepository.asyncData(relation));
                }
                for (Future<Boolean> future : resultList) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error("async db data to es exception", e);
                        }
                    }
                }
            }
        }, new CronTrigger(config.getCron()));
    }
}
