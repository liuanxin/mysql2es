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
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("NullableProblems")
@Configuration
@AllArgsConstructor
public class Job implements SchedulingConfigurer {

    private static final AtomicBoolean SYNC_RUN = new AtomicBoolean(false);
    private static final AtomicBoolean COMPENSATE_RUN = new AtomicBoolean(false);

    private final Config config;
    private final DataRepository dataRepository;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        Map<String, Runnable> taskMap = Maps.newHashMap();
        taskMap.put(config.getCron(), sync());
        if (config.isEnableCompensate()) {
            taskMap.put(config.getCompensateCron(), compensate());
        }

        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(taskMap.size());
        taskScheduler.initialize();
        taskRegistrar.setTaskScheduler(taskScheduler);

        for (Map.Entry<String, Runnable> entry : taskMap.entrySet()) {
            taskRegistrar.addTriggerTask(entry.getValue(), new CronTrigger(entry.getKey()));
        }
    }

    private Runnable sync() {
        return () -> {
            if (!config.isEnable()) {
                return;
            }
            if (!SYNC_RUN.compareAndSet(false, true)) {
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
                                String tps = (count > 0 && ms > 0) ? String.valueOf(count * 1000 / ms) : "0";
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
                SYNC_RUN.set(false);
            }
        };
    }

    private Runnable compensate() {
        return () -> {
            if (!config.isEnable()) {
                return;
            }
            if (!COMPENSATE_RUN.compareAndSet(false, true)) {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("compensate task has be running");
                }
                return;
            }

            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("compensate begin to run task");
            }
            long start = System.currentTimeMillis();
            try {
                IncrementStorageType incrementType = config.getIncrementType();
                int second = config.getCompensateSecond();
                Map<String, Future<Long>> resultMap = Maps.newHashMap();
                for (Relation relation : config.getRelation()) {
                    resultMap.put(relation.useKey(), dataRepository.asyncCompensateData(incrementType, relation, second));
                }
                for (Map.Entry<String, Future<Long>> entry : resultMap.entrySet()) {
                    try {
                        Long count = entry.getValue().get();
                        if (U.isNotBlank(count)) {
                            if (Logs.ROOT_LOG.isInfoEnabled()) {
                                long ms = System.currentTimeMillis() - start;
                                String tps = (count > 0 && ms > 0) ? String.valueOf(count * 1000 / ms) : "0";
                                Logs.ROOT_LOG.info("compensate async({}) count({}) time({}) tps({})",
                                        entry.getKey(), count, Dates.toHuman(ms), tps);
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error(String.format("compensate async(%s) Thread exception, time(%s)",
                                    entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                        }
                    } catch (Exception e) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error(String.format("compensate async(%s) exception, time(%s)",
                                    entry.getKey(), Dates.toHuman(System.currentTimeMillis() - start)), e);
                        }
                    }
                }
            } finally {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("compensate end of run task, time({})", Dates.toHuman(System.currentTimeMillis() - start));
                }
                COMPENSATE_RUN.set(false);
            }
        };
    }
}
