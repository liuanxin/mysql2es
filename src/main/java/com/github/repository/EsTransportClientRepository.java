package com.github.repository;

import com.github.model.Config;
import com.github.model.Document;
import com.github.model.Scheme;
import com.github.util.A;
import com.github.util.Jsons;
import com.github.util.Logs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

// @Component
public class EsTransportClientRepository {

    private final Config config;
    private final TransportClient client;

     // @Autowired
     public EsTransportClientRepository(Config config, TransportClient client) {
         this.config = config;
         this.client = client;
     }

     public boolean deleteScheme(List<Scheme> schemes) {
        if (A.isNotEmpty(schemes)) {
            IndicesAdminClient indices = client.admin().indices();

            for (Scheme scheme : schemes) {
                String index = scheme.getIndex();
                String type = scheme.getType();
                try {
                    if (indices.prepareExists(index).get().isExists()) {
                        DeleteIndexResponse resp = indices.prepareDelete(index).get();
                        boolean flag = resp.isAcknowledged();
                        if (Logs.ROOT_LOG.isDebugEnabled()) {
                            Logs.ROOT_LOG.debug("client => delete scheme ({}/{}) return: ({})", index, type, flag);
                        }
                    }
                } catch (ElasticsearchException e) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn(String.format("client => delete scheme (%s/%s) es exception", index, type), e);
                    }
                }
            }
        }
        return true;
    }

    public boolean saveScheme(List<Scheme> schemes) {
        if (A.isNotEmpty(schemes)) {
            List<Scheme> successList = A.lists();

            IndicesAdminClient indices = client.admin().indices();
            for (Scheme scheme : schemes) {
                // begin with 6.0, 1 index can't set multi type
                // Rejecting mapping update to [index] as the final mapping would have more than 1 type [type1, type2]
                String index = scheme.getIndex();
                String type = scheme.getType();
                try {
                    // create index if not exists
                    if (!indices.prepareExists(index).get().isExists()) {
                        indices.prepareCreate(index).get();
                    }
                    // indices.prepareDelete(index).get().isAcknowledged();

                    String source = Jsons.toJson(A.maps("properties", scheme.getProperties()));
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("client => curl -XPUT \"http://{}/{}/{}/_mapping\" -d '{}'",
                                config.ipAndPort(), index, type, source);
                    }
                    // create or update mapping
                    PutMappingResponse resp = indices.preparePutMapping(index).setType(type)
                            .setSource(source, XContentType.JSON).get();
                    boolean flag = resp.isAcknowledged();
                    if (flag) {
                        successList.add(scheme);
                    }
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("client => put scheme ({}/{}) return: ({})", index, type, flag);
                    }
                } catch (Exception e) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn(String.format("client => put scheme (%s/%s) es exception", index, type), e);
                    }
                }
            }

            if (A.isNotEmpty(successList)) {
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("client => put {} schemes ({}) from db to es",
                            successList.size(), Jsons.toJson(successList));
                }
            }
        }
        return true;
    }

    public boolean saveDataToEs(List<Document> documents) {
        if (A.isNotEmpty(documents)) {
            List<Document> successList = A.lists();
            for (Document doc : documents) {
                try {
                    IndexResponse response = client.prepareIndex(doc.getIndex(), doc.getType(), doc.getId())
                            .setSource(Jsons.toJson(doc.getData()), XContentType.JSON).get();
                    if (Logs.ROOT_LOG.isDebugEnabled()) {
                        Logs.ROOT_LOG.debug("client => create or update date return : {}", response.status());
                    }

                    successList.add(doc);
                } catch (Exception e) {
                    if (Logs.ROOT_LOG.isWarnEnabled()) {
                        Logs.ROOT_LOG.warn("client => create or update data es exception", e);
                    }
                }
            }

            if (A.isNotEmpty(successList)) {
                if (Logs.ROOT_LOG.isDebugEnabled()) {
                    Logs.ROOT_LOG.debug("client => put {} {} documents from db to es", successList.size(), successList);
                }
            }
        }
        return true;
    }
}

