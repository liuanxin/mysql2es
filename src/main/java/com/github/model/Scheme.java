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
public class Scheme {

    // begin with 6.0, type will be remove, replace with _doc
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html

    String index;
    String type = "_doc";
    Map<String, Map> properties;
}
