package com.github.mte.service;

import com.github.mte.BaseTest;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.test.context.jdbc.Sql;

@SpringBootApplication(scanBasePackages = "com.github.mte")
public class DataTest extends BaseTest {

    @Autowired
    private BondingService bondingService;

    @Sql({"classpath:sql/scheme.sql", "classpath:sql/insert.sql"})
    @Sql(value = {"classpath:sql/delete.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    @Test
    public void test() {
        bondingService.saveOrUpdateData();
    }

    @After
    public void deleteTempFile() {
        bondingService.deleteScheme();
        bondingService.deleteTempFile();
    }
}
