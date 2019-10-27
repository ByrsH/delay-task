package com.byrsh.delaytask.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 18:37 2019/8/17
 * @Modified By:
 */
public final class JsonMapper {

    private static final Logger LOGGER = LogManager.getLogger(JsonMapper.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final byte[] EMPTY_BYTES = new byte[0];

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private JsonMapper() {
    }

    public static byte[] writeValueAsBytes(Object object) {
        byte[] json = EMPTY_BYTES;
        try {
            json = MAPPER.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            LOGGER.warn("object to json bytes error", e);
        }
        return json;
    }

    public static String writeValueAsString(Object object) {
        String json = "";
        try {
            json = MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOGGER.warn("object to json string error", e);
        }
        return json;
    }

    public static <T> T readValue(byte[] data, Class<T> clazz) {
        T t = null;
        if (data != null && data.length != 0) {
            try {
                t = MAPPER.readValue(data, clazz);
            } catch (IOException e) {
                LOGGER.warn("json bytes to object error", e);
            }
        }
        return t;
    }

    public static <T> T readValue(String json, Class<T> clazz) {
        T t = null;
        if (json != null && json.length() > 0) {
            try {
                t = MAPPER.readValue(json, clazz);
            } catch (IOException e) {
                LOGGER.warn("json string to object error", e);
            }
        }
        return t;
    }

    public static <T> T readValue(String json, TypeReference valueTypeRef) {
        T t = null;
        if (json != null && json.length() > 0) {
            try {
                t = MAPPER.readValue(json, valueTypeRef);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return t;
    }

}
