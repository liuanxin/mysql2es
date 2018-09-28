package com.github;

import com.github.model.Document;
import com.github.model.Scheme;
import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DataTest {

    @Autowired
    private EsRepository esRepository;

    // @Autowired
    // private EsTransportClientRepository esTransportClientRepository;

    @Autowired
    private DataRepository dataRepository;

    @Sql({"classpath:sql/scheme.sql", "classpath:sql/insert.sql"})
    @Sql(value = {"classpath:sql/delete.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    @Test
    public void test() {
        List<Document> documents = dataRepository.incrementData();
        boolean saveDataToEsFlag = esRepository.saveDataToEs(documents);
        if (saveDataToEsFlag) {
            List<Scheme> schemeList = dataRepository.dbToEsScheme();
            boolean deleteSchemeFlag = esRepository.deleteScheme(schemeList);
            if (deleteSchemeFlag) {
                dataRepository.deleteTempFile();
            }
        }

        /*
        List<Document> documents = dataRepository.incrementData();
        boolean saveDataToEsFlag = esTransportClientRepository.saveDataToEs(documents);
        if (saveDataToEsFlag) {
            List<Scheme> schemeList = dataRepository.dbToEsScheme();
            boolean deleteSchemeFlag = esTransportClientRepository.deleteScheme(schemeList);
            if (deleteSchemeFlag) {
                dataRepository.deleteTempFile();
            }
        }
        */
    }
}
