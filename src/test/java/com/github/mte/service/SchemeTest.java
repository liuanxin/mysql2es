package com.github.mte.service;

import com.github.mte.BaseTest;
import com.github.mte.repository.DataRepository;
import com.github.mte.util.U;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.test.context.jdbc.Sql;

@SpringBootApplication(scanBasePackages = "com.github.mte")
public class SchemeTest extends BaseTest {

    @Autowired
    private DataRepository dataRepository;
    @Autowired
    private BondingService bondingService;

    @Sql({"classpath:sql/scheme.sql"})
    @Sql(value = {"classpath:sql/delete.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    @Test
    public void test() {
        dataRepository.syncData(U.EMPTY);
    }

    @After
    public void deleteTempFile() {
        bondingService.deleteScheme();
    }
}
