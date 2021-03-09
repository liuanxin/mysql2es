package com.github.run;

import com.github.model.Config;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.util.Dates;
import com.github.util.F;
import com.github.util.Logs;
import com.github.util.U;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@Configuration
@AllArgsConstructor
public class Job implements SchedulingConfigurer {

    private static final AtomicBoolean RUN = new AtomicBoolean(false);

    private final Config config;
    private final DataRepository dataRepository;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(() -> {
            if (!config.isEnable()) {
                return;
            }
            if (!RUN.compareAndSet(false, true)) {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("task has be running");
                }
                return;
            }

            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("begin to run task");
            }
            long start = System.currentTimeMillis();
            try {
                IncrementStorageType incrementType = config.getIncrementType();
                boolean deleteEveryTime = incrementType == IncrementStorageType.TEMP_FILE && config.isDeleteTempEveryTime();
                Map<String, Future<Long>> resultMap = Maps.newHashMap();
                for (Relation relation : config.getRelation()) {
                    resultMap.put(relation.useKey(), dataRepository.asyncData(incrementType, relation));

                    if (deleteEveryTime) {
                        F.delete(relation.getTable(), relation.getIndex());
                    }
                }
                for (Map.Entry<String, Future<Long>> entry : resultMap.entrySet()) {
                    try {
                        Long count = entry.getValue().get();
                        if (U.isNotBlank(count)) {
                            if (Logs.ROOT_LOG.isInfoEnabled()) {
                                long ms = System.currentTimeMillis() - start;
                                String tps;
                                if (count > 0 && ms > 0) {
                                    tps = new BigDecimal(count * 1000)
                                            .divide(new BigDecimal(ms), 0, RoundingMode.DOWN).toString();
                                } else {
                                    tps = "0";
                                }
                                Logs.ROOT_LOG.info("async({}) count({}) time({}) tps({})",
                                        entry.getKey(), count, Dates.toHuman(ms), tps);
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error(String.format("async(%s) Thread exception, time(%s)",
                                    entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                        }
                    } catch (Exception e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error(String.format("async(%s) exception, time(%s)",
                                    entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                        }
                    }
                }
            } finally {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("end of run task, time({})", Dates.toHuman(System.currentTimeMillis() - start));
                }
                RUN.set(false);
            }
        }, new CronTrigger(config.getCron()));
    }
}
