package com.github.run;

import com.github.model.Scheme;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import com.github.util.Logs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;

@Profile("!test")
@Configuration
public class Runner implements ApplicationRunner {

    private final EsRepository esRepository;
    private final DataRepository dataRepository;

    @Autowired
    public Runner(EsRepository esRepository, DataRepository dataRepository) {
        this.esRepository = esRepository;
        this.dataRepository = dataRepository;
    }

    // @Autowired
    // private EsTransportClientRepository esTransportClientRepository;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("begin to generate scheme");
        }
        try {
            List<Scheme> schemeList = dataRepository.dbToEsScheme();
            esRepository.saveScheme(schemeList);
            // esTransportClientRepository.saveScheme(schemeList);
        } finally {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("end of generate scheme");
            }
        }
    }
}
