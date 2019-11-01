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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

@Component
public class EsRepository {

    private final RestHighLevelClient client;
    public EsRepository(RestHighLevelClient client) {
        this.client = client;
    }


    @Async
    public Future<Boolean> deleteScheme(String index, String type) {
        IndicesClient indices = client.indices();

        try {
            if (indices.exists(new GetIndexRequest().indices(index))) {
                long start = System.currentTimeMillis();
                DeleteIndexResponse resp = indices.delete(new DeleteIndexRequest(index));
                boolean flag = resp.isAcknowledged();
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("delete scheme ({}/{}) time({}) return({})",
                            index, type, (System.currentTimeMillis() - start + "ms"), flag);
                }
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isWarnEnabled()) {
                Logs.ROOT_LOG.warn(String.format("delete scheme (%s/%s) es exception", index, type), e);
            }
        }
        return new AsyncResult<>(true);
    }


    public void saveScheme(String index, String type, Map<String, Map> properties) {
        IndicesClient indices = client.indices();

        boolean exists = exists(indices, index);
        if (exists) {
            return;
        }
        boolean create = createIndex(indices, index);
        if (!create) {
            return;
        }
        createScheme(indices, index, type, properties);
    }
    private boolean exists(IndicesClient indices, String index) {
        try {
            long start = System.currentTimeMillis();
            boolean ack = indices.exists(new GetIndexRequest().indices(index));
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("query index({}) exists time({}) return({})",
                        index, (System.currentTimeMillis() - start + "ms"), ack);
            }
            return ack;
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("query index(%s) exists exception", index), e);
            }
            return true;
        }
    }
    private boolean createIndex(IndicesClient indices, String index) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(index);
            // String settings = Searchs.getSettings();
            // request.settings(settings, XContentType.JSON);
            long start = System.currentTimeMillis();
            boolean ack = indices.create(request).isAcknowledged();
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("create index({}) time({}) return({})",
                        index, (System.currentTimeMillis() - start + "ms"), ack);
            }
            return ack;
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
            }
            return false;
        }
    }
    private void createScheme(IndicesClient indices, String index, String type, Map<String, Map> properties) {
        try {
            String source = Jsons.toJson(A.maps("properties", properties));
            PutMappingRequest request = new PutMappingRequest(index).type(type).source(source, XContentType.JSON);
            long start = System.currentTimeMillis();
            boolean ack = indices.putMapping(request).isAcknowledged();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("put ({}/{}) mapping time({}) return({})",
                        index, type, (System.currentTimeMillis() - start + "ms"), ack);
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
            }
        }
    }


    public int saveDataToEs(String index, String type, Map<String, String> idDataMap) {
        if (A.isEmpty(idDataMap)) {
            return 0;
        } else {
            BulkRequest batchRequest = new BulkRequest();
            for (Map.Entry<String, String> entry : idDataMap.entrySet()) {
                String id = entry.getKey(), source = entry.getValue();
                if (U.isNotBlank(id) && U.isNotBlank(source)) {
                    batchRequest.add(new IndexRequest(index, type, id).source(source, XContentType.JSON));
                }
            }

            try {
                BulkResponse responses = client.bulk(batchRequest);
                int size = responses.getItems().length;
                for (BulkItemResponse response : responses) {
                    if (response.isFailed()) {
                        size--;
                    }
                }
                return size;
            } catch (IOException e) {
                // <= 6.3.1 version, suggest field if empty will throw IllegalArgumentException(write is good)
                // org.elasticsearch.index.mapper.CompletionFieldMapper.parse(443)
                // https://github.com/elastic/elasticsearch/pull/30713/files
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error(String.format("create or update (%s/%s) es data exception", index, type), e);
                }
                return 0;
            }
        }
    }
}
