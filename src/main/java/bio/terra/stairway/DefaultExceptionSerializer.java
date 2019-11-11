package bio.terra.stairway;

import bio.terra.stairway.exception.FlightException;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * This is the default implementation of ExceptionSerializer. It is used to convert to and from exceptions.
 * It accepts an Exception in and saves the class name and message.
 * It accepts name and message and carefully generates an exception result.
 *
 * The following requirements must be met to generate an exception from the name and message:
 * <ol>
 * <li> The exception class must exist </li>
 * <li> The exception class must be a RuntimeException </li>
 * <li> The exception must have a constructor accepting a single string argument </li>
 * </ol>
 *
 * If those requirements are not met, then we generate the UnmappableException to hold the
 * class name string and message string.
 */
class DefaultExceptionSerializer implements ExceptionSerializer {
    private static final String SEPARATOR = ";";

    @Override
    public String serialize(Exception exception) {
        if (exception == null) {
            return StringUtils.EMPTY;
        }
        return exception.getClass().getName() + SEPARATOR + exception.getMessage();
    }

    @Override
    public Exception deserialize(String serializedException) {
        if (StringUtils.isEmpty(serializedException)) {
            return null;
        }

        String exceptionClass = StringUtils.substringBefore(serializedException, SEPARATOR);
        String exceptionMessage = StringUtils.substringAfter(serializedException, SEPARATOR);

        if (StringUtils.isEmpty(exceptionClass)) {
            return null;
        }

        try {
            Class<?> clazz = Class.forName(exceptionClass);
            Constructor<?> ctor = clazz.getConstructor(String.class);
            Object object = ctor.newInstance(exceptionMessage);

            if (object instanceof RuntimeException) {
                return (Exception) object;
            }
        } catch (ClassNotFoundException |
            NoSuchMethodException |
            InstantiationException |
            IllegalAccessException |
            InvocationTargetException ex) {
            // Fall through to common exit code
        }
        return new FlightException("Exception class: " + exceptionClass +
            "; Exception message: " + exceptionMessage);
    }
}
