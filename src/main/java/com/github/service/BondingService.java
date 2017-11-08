package com.github.service;

import com.github.repository.DataRepository;
import com.github.repository.EsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Component
public class BondingService {

    @Autowired
    private EsRepository esRepository;
    @Autowired
    private DataRepository dataRepository;

    public boolean createScheme() {
        Future<Boolean> result = esRepository.saveScheme(dataRepository.dbToEsScheme());
        try {
            return result.get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    public boolean syncData() {
        Future<Boolean> result = esRepository.saveDataToEs(dataRepository.incrementData());
        try {
            return result.get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    public boolean deleteScheme() {
        Future<Boolean> result = esRepository.deleteScheme(dataRepository.dbToEsScheme());
        try {
            return result.get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    public void deleteTempFile() {
        dataRepository.deleteTempFile();
    }
}
