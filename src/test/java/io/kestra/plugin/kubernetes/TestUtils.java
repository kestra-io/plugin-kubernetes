package io.kestra.plugin.kubernetes;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.JacksonMapper;

public abstract class TestUtils {
    private static final ObjectMapper mapper = JacksonMapper.ofYaml();

    public static <T> Map<String, Object> convert(Class<T> cls, String... yaml) throws JsonProcessingException {
        return JacksonMapper.toMap(mapper.readValue(String.join("\n", yaml), cls));
    }
}
