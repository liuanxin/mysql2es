package com.github.repository;

import com.github.model.Config;
import com.github.model.Document;
import com.github.model.Scheme;
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
import java.util.Map;
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

    public boolean saveScheme(List<Scheme> schemes) {
        if (A.isNotEmpty(schemes)) {
            Map<String, Boolean> map = A.maps();

            IndicesClient indices = client.indices();
            for (Scheme scheme : schemes) {
                String index = scheme.getIndex();
                String type = scheme.getType();

                if (!exists(indices, index)) {
                    if (createIndex(indices, index)) {
                        try {
                            boolean ack = createScheme(indices, scheme);
                            map.put(index + "/" + type, ack);
                        } catch (IOException e) {
                            if (Logs.ROOT_LOG.isErrorEnabled()) {
                                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
                            }
                        }
                    }
                }
            }

            if (A.isNotEmpty(map)) {
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("put {} ({}) schemes from db to es", map.size(), Jsons.toJson(map));
                }
            }
        }
        return true;
    }

    private boolean exists(IndicesClient indices, String index) {
        try {
            return indices.exists(new GetIndexRequest().indices(index));
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("query index(%s) exists exception", index), e);
            }
            return true;
        }
    }

    private boolean createIndex(IndicesClient indices, String index) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        /*
        String settings = Searchs.getSettings();
        request.settings(settings, XContentType.JSON);
        */
        try {
            boolean ack = indices.create(request).isAcknowledged();
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("create index({}): {}", index, ack);
            }
            return ack;
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("create index(%s) exception", index), e);
            }
            return false;
        }
    }

    private boolean createScheme(IndicesClient indices, Scheme scheme) throws IOException {
        String index = scheme.getIndex();
        String type = scheme.getType();

        String source = Jsons.toJson(A.maps("properties", scheme.getProperties()));
        if (Logs.ROOT_LOG.isDebugEnabled()) {
            Logs.ROOT_LOG.debug("curl -XPUT \"http://{}/{}/{}/_mapping\" -d '{}'", config.ipAndPort(), index, type, source);
        }

        if (U.isBlank(source)) {
            return false;
        } else {
            PutMappingRequest request = new PutMappingRequest(index).type(type).source(source, XContentType.JSON);
            boolean ack = indices.putMapping(request).isAcknowledged();
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("put ({}/{}) mapping: {}", index, type, ack);
            }
            return ack;
        }
    }

    public void saveDataToEs(boolean justAdd, List<Document> documents) {
        if (A.isNotEmpty(documents)) {
            BulkRequest batchRequest = new BulkRequest();
            for (Document doc : documents) {
                String index = doc.getIndex();
                String type = doc.getType();
                String id = doc.getId();
                String source = Jsons.toJson(doc.getData());
                if (U.isNotBlank(source)) {
                    if (justAdd) {
                        batchRequest.add(new IndexRequest(index, type, id).source(source, XContentType.JSON));
                    } else {
                        try {
                            // es not have (add if not exists, update if exists)'s api...
                            boolean exists = client.exists(new GetRequest(index, type, id));
                            if (exists) {
                                batchRequest.add(new UpdateRequest(index, type, id).doc(source, XContentType.JSON));
                            } else {
                                batchRequest.add(new IndexRequest(index, type, id).source(source, XContentType.JSON));
                            }
                        } catch (Exception e) {
                            if (Logs.ROOT_LOG.isErrorEnabled()) {
                                Logs.ROOT_LOG.error(String.format("es query %s/%s/%s exists exception", index, type, id), e);
                            }
                        }
                    }
                }
            }
            try {
                BulkResponse bulk = client.bulk(batchRequest);
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("batch(" + bulk.getItems().length + ") success.");
                }
            } catch (IOException e) {
                // <= 6.3.1 version, suggest field if empty will throw IAE(write is good)
                // org.elasticsearch.index.mapper.CompletionFieldMapper.parse(443)
                // https://github.com/elastic/elasticsearch/pull/30713/files
                if (Logs.ROOT_LOG.isErrorEnabled()) {
                    Logs.ROOT_LOG.error("create or update data es exception", e);
                }
            }
        }
    }
}
