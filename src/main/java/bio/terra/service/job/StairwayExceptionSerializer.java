package bio.terra.service.job;

import bio.terra.common.exception.DataRepoException;
import bio.terra.service.job.exception.JobResponseException;
import bio.terra.service.job.exception.SerDesException;
import bio.terra.stairway.ExceptionSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class StairwayExceptionSerializer implements ExceptionSerializer {
    private ObjectMapper objectMapper;

    public StairwayExceptionSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String serialize(Exception exception) {
        if (exception == null) {
            return StringUtils.EMPTY;
        }

        // Wrap non-runtime exceptions so they can be rethrown later
        if (!(exception instanceof RuntimeException)) {
            exception = new JobResponseException(exception.getMessage(), exception);
        }

        StairwayExceptionFields fields = new StairwayExceptionFields();

        if (exception instanceof DataRepoException) {
            fields
                .setDataRepoException(true)
                .setClassName(exception.getClass().getName())
                .setMessage(exception.getMessage())
                .setErrorDetails(((DataRepoException) exception).getErrorDetails());
        } else {
            fields
                .setDataRepoException(false)
                .setClassName(exception.getClass().getName())
                .setMessage(exception.getMessage());
        }

        try {
            return objectMapper.writeValueAsString(fields);
        } catch (JsonProcessingException ex) {
            throw new SerDesException("This should never happen", ex);
        }
    }

    @Override
    public Exception deserialize(String serializedException) {
        if (StringUtils.isEmpty(serializedException)) {
            return null;
        }

        // Decode the exception fields from JSON
        StairwayExceptionFields fields;
        try {
            fields = objectMapper.readValue(serializedException, StairwayExceptionFields.class);
        } catch (IOException ex) {
            // objectMapper exceptions
            return new SerDesException("Failed to deserialize exception data: " + serializedException, ex);
        }

        // Find the class from the class name
        Class<?> clazz;
        try {
            clazz = Class.forName(fields.getClassName());
        } catch (ClassNotFoundException ex) {
            return new SerDesException("Exception class not found: " + fields.getClassName() +
                "; Exception message: " + fields.getMessage());
        }

        // If this is a data repo exception and the exception exposes a constructor with the
        // error details, then we try to use that.
        if (fields.isDataRepoException()) {
            try {
                Constructor<?> ctor = clazz.getConstructor(String.class, List.class);
                Object object = ctor.newInstance(fields.getMessage(), fields.getErrorDetails());
                return (Exception) object;
            } catch (NoSuchMethodException |
                SecurityException |
                InstantiationException |
                IllegalAccessException |
                IllegalArgumentException |
                InvocationTargetException ex) {
                // We didn't find a constructor with error details or construction failed. Fall through
            }
        }

        // We have either a data repo exception that doesn't support error details or some other runtime exception
        try {
            Constructor<?> ctor = clazz.getConstructor(String.class);
            Object object = ctor.newInstance(fields.getMessage());
            return (Exception)object;
        } catch (NoSuchMethodException |
            SecurityException |
            InstantiationException |
            IllegalAccessException |
            IllegalArgumentException |
            InvocationTargetException ex) {
            // We didn't find a vanilla constructor or construction failed. Fall through
        }

        return new SerDesException("Failed to construct exception: " + fields.getClassName() +
            "; Exception message: " + fields.getMessage());
    }
}
