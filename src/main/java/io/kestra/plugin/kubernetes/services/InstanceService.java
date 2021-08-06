package io.kestra.plugin.kubernetes.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.MapUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class InstanceService {
    private static final ObjectMapper mapper = JacksonMapper.ofYaml();

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T fromMap(Class<T> cls, RunContext runContext, Map<String, Object> map) throws IOException, IllegalVariableEvaluationException {
        Map<Object, Object> render = render(runContext, (Map) map);

        String yaml = JacksonMapper.ofYaml().writeValueAsString(render);
        return mapper.readValue(yaml, cls);
    }

    public static <T> T fromMap(Class<T> cls, RunContext runContext, Map<String, Object> map, Map<String, Object> defaults) throws IOException, IllegalVariableEvaluationException {
        return fromMap(cls, runContext, MapUtils.merge(map, defaults));
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> render(RunContext runContext, Map<Object, Object> map) throws IllegalVariableEvaluationException {
        Map<Object, Object> copy = new HashMap<>();

        for (Object key : map.keySet()) {
            Object value = map.get(key);
            if (key instanceof String) {
                key = runContext.render((String) key);
            }

            if (value instanceof String) {
                value = runContext.render((String) value);
            }

            if (value instanceof Map) {
                copy.put(key, render(runContext, (Map<Object, Object>) value));
            }
            else if (value instanceof List) {
                copy.put(key, render(runContext, (List<Object>) value));
            }
            else {
                copy.put(key, value);
            }

        }

        return copy;
    }

    @SuppressWarnings({"rawtypes"})
    private static List render(RunContext runContext, List list) throws IllegalVariableEvaluationException {
        List<Object> copy = new ArrayList<>();

        for (Object o : list) {
            copy.add(renderVar(runContext, o));
        }

        return copy;
    }

    @SuppressWarnings("unchecked")
    private static Object renderVar(RunContext runContext, Object value) throws IllegalVariableEvaluationException {
        if (value instanceof String) {
            return runContext.render((String) value);
        }

        if (value instanceof Map) {
            return render(runContext, (Map<Object, Object>) value);
        }

        else if (value instanceof List) {
            return render(runContext, (List<Object>) value);
        }

        return value;
    }
}
