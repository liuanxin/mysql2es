package com.github.repository;

import com.github.model.Config;
import com.github.util.A;
import com.github.util.Jsons;
import com.github.util.Logs;
import com.github.util.U;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

@Component
@RequiredArgsConstructor
@SuppressWarnings("rawtypes")
public class EsRepository {

    private final Config config;
    private final RestHighLevelClient client;

    @Async
    public Future<Boolean> deleteScheme(String index) {
        if (!config.isEnable()) {
            return new AsyncResult<>(false);
        }
        try {
            IndicesClient indices = client.indices();
            if (indices.exists(new GetIndexRequest().indices(index), RequestOptions.DEFAULT)) {
                long start = System.currentTimeMillis();
                AcknowledgedResponse resp = indices.delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                boolean flag = resp.isAcknowledged();
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("delete scheme ({}) time({}) return({})", index, (System.currentTimeMillis() - start + "ms"), flag);
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
        if (!config.isEnable()) {
            throw new RuntimeException("break sync db to es");
        }
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
            boolean ack = indices.exists(new GetIndexRequest().indices(index), RequestOptions.DEFAULT);
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
                Logs.ROOT_LOG.error(String.format("put index(%s) mapping exception", index), e);
            }
        }
    }

    /** { key1 : { id1 : data1, id2 : data2 }, key2 : { id3 : data3, id4 : data4 } } */
    public int saveDataToEs(Map<String, Map<String, Map<String, String>>> indexIdDataMap) {
        if (!config.isEnable()) {
            throw new RuntimeException("break sync db to es");
        }
        if (A.isEmpty(indexIdDataMap)) {
            return 0;
        }

        BulkRequest batchRequest = new BulkRequest();
        long originalSize = 0;
        for (Map.Entry<String, Map<String, Map<String, String>>> entry : indexIdDataMap.entrySet()) {
            String index = entry.getKey();
            for (Map.Entry<String, Map<String, String>> dataEntry : entry.getValue().entrySet()) {
                String id = dataEntry.getKey();
                Map<String, String> source = dataEntry.getValue();
                if (U.isNotBlank(id) && A.isNotEmpty(source)) {
                    IndexRequest doc = new IndexRequest(index).id(id);
                    String data = source.get("data");
                    if (U.isNotBlank(data)) {
                        doc.source(data, XContentType.JSON);

                        String routing = source.get("routing");
                        if (U.isNotBlank(routing)) {
                            doc.routing(routing);
                        }

                        if (config.isVersionCheck()) {
                            Long version = U.toLong(source.get("version"));
                            if (U.greater0(version)) {
                                doc.versionType(VersionType.EXTERNAL_GTE).version(version);
                            }
                        }

                        batchRequest.add(doc);
                        originalSize++;
                    }
                }
            }
        }

        try {
            BulkResponse responses = client.bulk(batchRequest, RequestOptions.DEFAULT);
            int size = responses.getItems().length;

            for (BulkItemResponse response : responses) {
                if (response.isFailed()) {
                    BulkItemResponse.Failure failure = response.getFailure();
                    if (!"version_conflict_engine_exception".equals(failure.getType())) {
                        if (Logs.ROOT_LOG.isErrorEnabled()) {
                            Logs.ROOT_LOG.error("batch save({}) size({}) success({}), has error({})",
                                    response.getIndex(), originalSize, size, failure);
                        }
                    }
                    size--;
                }
            }
            if (size == originalSize) {
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("batch save es({}) size({}) success({})", indexIdDataMap.keySet(), originalSize, size);
                }
            }
            return size;
        } catch (IOException e) {
            // <= 6.3.1 version, suggest field if empty will throw IllegalArgumentException(write is good)
            // org.elasticsearch.index.mapper.CompletionFieldMapper.parse(443)
            // https://github.com/elastic/elasticsearch/pull/30713/files
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create or update es(%s) data exception", indexIdDataMap.keySet()), e);
            }
            throw new RuntimeException("save to es exception:" + e.getMessage());
        }
    }
}
