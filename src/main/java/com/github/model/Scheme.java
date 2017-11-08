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
public class Scheme {
    String index;
    String type;
    Map<String, Map> properties;

    @Override
    public String toString() {
        return index + "/" + type;
    }
}
