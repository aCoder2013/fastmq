package com.song.fastmq.common.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by song on 2017/11/5.
 */
public class JsonUtils {

    private static final ThreadLocal<ObjectMapper> mapperFactory = ThreadLocal.withInitial(() -> {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        return mapper;
    });

    public static ObjectMapper get() {
        return mapperFactory.get();
    }
}
