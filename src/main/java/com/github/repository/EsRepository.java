package com.github.repository;

import com.github.util.A;
import com.github.util.Jsons;
import com.github.util.Logs;
import com.github.util.U;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

@Component
@SuppressWarnings("rawtypes")
public class EsRepository {

    private final RestHighLevelClient client;
    public EsRepository(RestHighLevelClient client) {
        this.client = client;
    }


    @Async
    public Future<Boolean> deleteScheme(String index) {
        IndicesClient indices = client.indices();

        try {
            if (indices.exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                long start = System.currentTimeMillis();
                AcknowledgedResponse resp = indices.delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                boolean flag = resp.isAcknowledged();
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("delete scheme ({}) time({}) return({})",
                            index, (System.currentTimeMillis() - start + "ms"), flag);
                }
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isWarnEnabled()) {
                Logs.ROOT_LOG.warn(String.format("delete scheme (%s) es exception", index), e);
            }
        }
        return new AsyncResult<>(true);
    }


    public void saveScheme(String index, Map<String, Map> properties) {
        IndicesClient indices = client.indices();

        boolean exists = existsIndex(indices, index);
        if (!exists) {
            boolean create = createIndex(indices, index);
            if (create) {
                createScheme(indices, index, properties);
            }
        }
    }
    private boolean existsIndex(IndicesClient indices, String index) {
        try {
            long start = System.currentTimeMillis();
            boolean ack = indices.exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
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
            boolean ack = indices.create(request, RequestOptions.DEFAULT).isAcknowledged();
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
    private void createScheme(IndicesClient indices, String index, Map<String, Map> properties) {
        try {
            String source = Jsons.toJson(A.maps("properties", properties));
            PutMappingRequest request = new PutMappingRequest(index).source(source, XContentType.JSON);
            long start = System.currentTimeMillis();
            boolean ack = indices.putMapping(request, RequestOptions.DEFAULT).isAcknowledged();
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info("put ({}) mapping time({}) return({})", index, (System.currentTimeMillis() - start + "ms"), ack);
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
            }
        }
    }


    public int saveDataToEs(String index, Map<String, String> idDataMap) {
        if (A.isEmpty(idDataMap)) {
            return 0;
        }

        BulkRequest batchRequest = new BulkRequest();
        long originalSize = 0;
        for (Map.Entry<String, String> entry : idDataMap.entrySet()) {
            String id = entry.getKey(), source = entry.getValue();
            if (U.isNotBlank(id) && U.isNotBlank(source)) {
                batchRequest.add(new IndexRequest(index).id(id).source(source, XContentType.JSON));
                originalSize++;
            }
        }

        try {
            BulkResponse responses = client.bulk(batchRequest, RequestOptions.DEFAULT);
            int size = responses.getItems().length;

            BulkItemResponse.Failure fail = null;
            for (BulkItemResponse response : responses) {
                if (response.isFailed()) {
                    fail = response.getFailure();
                    size--;
                }
            }
            if (size == originalSize) {
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("batch save({}) size({}) success({})", index, originalSize, size);
                }
            } else {
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error("batch save({}) size({}) success({}), has error({})", index, originalSize, size, fail);
                }
            }
            return size;
        } catch (IOException e) {
            // <= 6.3.1 version, suggest field if empty will throw IllegalArgumentException(write is good)
            // org.elasticsearch.index.mapper.CompletionFieldMapper.parse(443)
            // https://github.com/elastic/elasticsearch/pull/30713/files
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create or update (%s) es data exception", index), e);
            }
            return 0;
        }
    }
}
