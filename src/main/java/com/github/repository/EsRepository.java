package com.github.repository;

import com.github.model.Config;
import com.github.model.Document;
import com.github.model.Scheme;
import com.github.util.A;
import com.github.util.Jsons;
import com.github.util.Logs;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

@Component
public class EsRepository {

    @Autowired
    private Config config;

    @Autowired
    private RestHighLevelClient client;

    @Async
    public Future<Boolean> deleteScheme(List<Scheme> schemes) {
        if (A.isNotEmpty(schemes)) {
            IndicesClient indices = client.indices();

            for (Scheme scheme : schemes) {
                String index = scheme.getIndex();
                String type = scheme.getType();
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
            }
        }
        return new AsyncResult<>(true);
    }

    @Async
    public Future<Boolean> saveScheme(List<Scheme> schemes) {
        if (A.isNotEmpty(schemes)) {
            List<Scheme> successList = A.lists();

            IndicesClient indices = client.indices();
            for (Scheme scheme : schemes) {
                String index = scheme.getIndex();
                String type = scheme.getType();
                try {
                    // create index if not exists
                    if (!indices.exists(new GetIndexRequest().indices(index))) {
                        indices.create(new CreateIndexRequest(index));
                    }

                    String source = Jsons.toJson(A.maps("properties", scheme.getProperties()));
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("curl -XPUT \"http://{}/{}/{}/_mapping\" -d '{}'",
                                config.ipAndPort(), index, type, source);
                    }
                    PutMappingRequest put = new PutMappingRequest(index).type(type).source(source, XContentType.JSON);
                    PutMappingResponse response = indices.putMapping(put);
                    boolean flag = response.isAcknowledged();
                    if (flag) {
                        successList.add(scheme);
                    }
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("put scheme ({}/{}) return: ({})", index, type, flag);
                    }
                } catch (Exception e) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn(String.format("put scheme (%s/%s) es exception", index, type), e);
                    }
                }
            }

            if (A.isNotEmpty(successList)) {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("put {} ({}) schemes from db to es", successList.size(), Jsons.toJson(successList));
                }
            }
        }
        return new AsyncResult<>(true);
    }

    @Async
    public Future<Boolean> saveDataToEs(List<Document> documents) {
        if (A.isNotEmpty(documents)) {
            List<Document> successList = A.lists();

            for (Document doc : documents) {
                try {
                    DocWriteResponse response;
                    boolean exists = client.exists(new GetRequest(doc.getIndex(), doc.getType(), doc.getId()));
                    if (exists) {
                        UpdateRequest request = new UpdateRequest(doc.getIndex(), doc.getType(), doc.getId())
                                .doc(Jsons.toJson(doc.getData()), XContentType.JSON);
                        response = client.update(request);
                    } else {
                        IndexRequest request = new IndexRequest(doc.getIndex(), doc.getType(), doc.getId())
                                .source(Jsons.toJson(doc.getData()), XContentType.JSON);
                        response = client.index(request);
                    }
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("create or update date return : {}", response.getResult().getLowercase());
                    }

                    if (response.getResult() != DocWriteResponse.Result.NOOP
                            && response.getResult() != DocWriteResponse.Result.NOT_FOUND) {
                        successList.add(doc);
                    }
                } catch (Exception e) {
                    // <= 6.3.1 version, suggest field if empty will throw IAE(write is good)
                    // org.elasticsearch.index.mapper.CompletionFieldMapper.parse(443)
                    // https://github.com/elastic/elasticsearch/pull/30713/files
                    if (Logs.ROOT_LOG.isErrorEnabled()) {
                        Logs.ROOT_LOG.error("create or update data es exception", e);
                    }
                }
            }

            if (A.isNotEmpty(successList)) {
                if (Logs.ROOT_LOG.isInfoEnabled()) {
                    Logs.ROOT_LOG.info("put {} {} documents from db to es", successList.size(), successList);
                }
            }
        }
        return new AsyncResult<>(true);
    }
}
