package com.exascale.optimizer.externalTable;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.swift.util.JSONUtil;

import java.io.IOException;

/**
 * JSON Utils to serialize/deserialize json strings into parameters of External Table
 */
public class JSONUtils {

    private JSONUtils(){}

    public static void validate(String javaClassName, String jsonInString)
    {
        Class<?> javaClass;
        ExternalParamsInterface params;
        try {
            javaClass = Class.forName( javaClassName );
        } catch( ClassNotFoundException e ) {
            throw new RuntimeException("Java class " + javaClassName + " does not exist!");
        }
        if ( !ExternalTableType.class.isAssignableFrom(javaClass) ) {
            throw new RuntimeException("Class " + javaClassName + " does not implement ExternalTableType.");
        }

        ExternalTableType external;
        try {
            external = (ExternalTableType) javaClass.newInstance();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Validation error");
        } catch (InstantiationException e) {
            throw new RuntimeException("Validation error");
        }

        try {
            params = toObject(jsonInString, external.getParamsClass());
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Params property is not defined in input Java class");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Params class not found");
        } catch (IOException e) {
            throw new RuntimeException("Error conversion String to JSON: " + e.getMessage());
        }
        if (!params.valid()) {
            throw new RuntimeException("JSON parameters are not valid");
        }
    }

    public static <T> T toObject(String jsonInString, Class<T> klazz)
            throws IOException, ClassNotFoundException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonInString, klazz);
    }

}
