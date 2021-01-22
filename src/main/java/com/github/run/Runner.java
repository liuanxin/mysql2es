package com.github.run;

import com.github.model.Config;
import com.github.model.IncrementStorageType;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.A;
import com.github.util.Dates;
import com.github.util.Logs;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@AllArgsConstructor
public class Runner implements ApplicationRunner {

    private final Config config;
    private final EsRepository esRepository;
    private final DataRepository dataRepository;

    @SuppressWarnings({"RedundantThrows", "rawtypes"})
    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!config.isEnable()) {
            return;
        }

        if (config.getIncrementType() == IncrementStorageType.MYSQL) {
            dataRepository.generateIncrementTable();
        }

        long start = System.currentTimeMillis();
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("begin to generate scheme");
        }
        try {
            for (Relation relation : config.getRelation()) {
                Map<String, Map> properties = dataRepository.dbToEsScheme(relation);
                if (relation.isScheme() && A.isNotEmpty(properties)) {
                    String index = relation.useIndex();
                    esRepository.saveScheme(index, properties);
                }
            }
        } finally {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                long end = System.currentTimeMillis();
                Logs.ROOT_LOG.info("end of generate scheme, use time: {}.", Dates.toHuman(end - start));
            }
        }
    }
}
