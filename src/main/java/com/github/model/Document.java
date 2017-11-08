package com.github.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class Document {

    String index;
    String type;
    String id;
    Map<String, Object> data;

    @Override
    public String toString() {
        return index + "/" + type + "/" + id;
    }
}
