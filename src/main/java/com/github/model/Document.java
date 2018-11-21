package com.github.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public class Document {

    // begin with 6.0, type will be remove, replace with _doc
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html

    private String index;
    private String type = "_doc";
    private String id;
    private Map<String, Object> data;

    public Document(String index, String id, Map<String, Object> data) {
        this.index = index;
        this.id = id;
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format("/%s/%s/%s", index, type, id);
    }
}
