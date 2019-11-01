package com.github.run;

import com.github.model.Config;
import com.github.model.Relation;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.A;
import com.github.util.Logs;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Profile("!test")
@Configuration
public class Runner implements ApplicationRunner {

    private final Config config;
    private final EsRepository esRepository;
    private final DataRepository dataRepository;
    public Runner(Config config, EsRepository esRepository, DataRepository dataRepository) {
        this.config = config;
        this.esRepository = esRepository;
        this.dataRepository = dataRepository;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("begin to generate scheme");
        }

        try {
            for (Relation relation : config.getRelation()) {
                Map<String, Map> properties = dataRepository.dbToEsScheme(relation);
                if (relation.isScheme() && A.isNotEmpty(properties)) {
                    String index = relation.useIndex();
                    String type = relation.getType();
                    esRepository.saveScheme(index, type, properties);
                }
            }
        } finally {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("end of generate scheme");
            }
        }
    }
}
