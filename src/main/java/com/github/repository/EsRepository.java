package com.github.repository;

import com.github.util.A;
import com.github.util.Jsons;
import com.github.util.Logs;
import com.github.util.U;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

@Component
public class EsRepository {

    private final RestHighLevelClient client;

    @Autowired
    public EsRepository(RestHighLevelClient client) {
        this.client = client;
    }


    @Async
    public Future<Boolean> deleteScheme(String index, String type) {
        IndicesClient indices = client.indices();

        try {
            if (indices.exists(new GetIndexRequest().indices(index))) {
                DeleteIndexResponse resp = indices.delete(new DeleteIndexRequest(index));
                boolean flag = resp.isAcknowledged();
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("delete scheme ({}/{}) return: ({})", index, type, flag);
                }
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isWarnEnabled()) {
                Logs.ROOT_LOG.warn(String.format("delete scheme (%s/%s) es exception", index, type), e);
            }
        }
        return new AsyncResult<>(true);
    }


    public void saveScheme(String domain, String index, String type, Map<String, Map> properties) {
        IndicesClient indices = client.indices();

        boolean exists = exists(indices, domain, index);
        if (exists) {
            return;
        }
        boolean create = createIndex(indices, domain, index);
        if (!create) {
            return;
        }
        createScheme(indices, domain, index, type, properties);
    }
    private boolean exists(IndicesClient indices, String domain, String index) {
        try {
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("curl -I \"{}/{}\"", domain, index);
            }
            return indices.exists(new GetIndexRequest().indices(index));
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("query index(%s) exists exception", index), e);
            }
            return true;
        }
    }
    private boolean createIndex(IndicesClient indices, String domain, String index) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(index);
            // String settings = Searchs.getSettings();
            // request.settings(settings, XContentType.JSON);
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("curl -XPUT \"{}/{}\"", domain, index);
                // Logs.ROOT_LOG.debug("curl -X PUT \"{}/{}\" -d '{\"settings\":{}}'", domain, index, settings);
            }
            boolean ack = indices.create(request).isAcknowledged();
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("create index({}) return {}", index, ack);
            }
            return ack;
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
            }
            return false;
        }
    }
    private void createScheme(IndicesClient indices, String domain, String index, String type, Map<String, Map> properties) {
        try {
            String source = Jsons.toJson(A.maps("properties", properties));
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("curl -XPUT \"{}/{}/_mapping/_doc\" -d '{}'", domain, index, source);
            }
            PutMappingRequest request = new PutMappingRequest(index).type(type).source(source, XContentType.JSON);
            boolean ack = indices.putMapping(request).isAcknowledged();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("put ({}/{}) mapping return {}", index, type, ack);
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
            }
        }
    }


    public boolean saveDataToEs(String index, String type, Map<String, String> idDataMap) {
        if (A.isNotEmpty(idDataMap)) {
            BulkRequest batchRequest = new BulkRequest();
            for (Map.Entry<String, String> entry : idDataMap.entrySet()) {
                String id = entry.getKey(), source = entry.getValue();
                if (U.isNotBlank(id) && U.isNotBlank(source)) {
                    batchRequest.add(new IndexRequest(index, type, id).source(source, XContentType.JSON));
                }
            }
            try {
                BulkResponse bulk = client.bulk(batchRequest);
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("index({}) type({}) batch({}) success", index, type, bulk.getItems().length);
                }
                return true;
            } catch (IOException e) {
                // <= 6.3.1 version, suggest field if empty will throw IAE(write is good)
                // org.elasticsearch.index.mapper.CompletionFieldMapper.parse(443)
                // https://github.com/elastic/elasticsearch/pull/30713/files
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error(String.format("create or update index(%s) type(%s) es exception", index, type), e);
                }
            }
        }
        return false;
    }
}
