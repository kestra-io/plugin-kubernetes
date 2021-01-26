package org.kestra.task.kubernetes.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.JacksonMapper;

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
        return fromMap(cls, runContext, merge(map, defaults));
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> merge(Map<String, Object> map, Map<String, Object> defaults) {
        Map<String, Object> copy = map != null ? new HashMap<>(map) : new HashMap<>();

        for(String key : defaults.keySet()) {
            Object value2 = defaults.get(key);
            if (copy.containsKey(key)) {
                Object value1 = copy.get(key);
                if (value1 instanceof Map && value2 instanceof Map)
                    merge((Map<String, Object>) value1, (Map<String, Object>) value2);
                else if (value1 instanceof List && value2 instanceof List)
                    copy.put(key, merge((List<?>) value1, (List<?>) value2));
                else copy.put(key, value2);
            } else copy.put(key, value2);
        }

        return copy;
    }

    @SuppressWarnings({"unchecked", "rawtypes", "SuspiciousMethodCalls"})
    private static List merge(List list, List defaults) {
        List<?> copy = new ArrayList<>(defaults);

        copy.removeAll(list);
        copy.addAll(list);

        return copy;
    }
}
