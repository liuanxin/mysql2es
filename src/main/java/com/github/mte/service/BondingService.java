package com.github.mte.service;

import com.github.mte.repository.DataRepository;
import com.github.mte.repository.EsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BondingService {

    @Autowired
    private EsRepository esRepository;
    @Autowired
    private DataRepository dataRepository;

    public void createScheme() {
        esRepository.saveScheme(dataRepository.dbToEsScheme());
    }

    public void syncData() {
        esRepository.saveDataToEs(dataRepository.incrementData());
    }

    public void deleteScheme() {
        esRepository.deleteScheme(dataRepository.dbToEsScheme());
    }

    public void deleteTempFile() {
        dataRepository.deleteTempFile();
    }
}
