package com.github.run;

import com.github.model.Scheme;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;

@Profile("!test")
@Configuration
public class Runner implements ApplicationRunner {

    @Autowired
    private EsRepository esRepository;

    @Autowired
    private DataRepository dataRepository;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Scheme> schemeList = dataRepository.dbToEsScheme();
        esRepository.saveScheme(schemeList);
    }
}
