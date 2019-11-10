package bio.terra.stairway;

import bio.terra.stairway.exception.UnmappableException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * This is a helper class for the FlightDao. It is used to convert to and from exceptions.
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
class ExceptionFields {
    private String exceptionClass;
    private String exceptionMessage;

    ExceptionFields(Exception exception) {
        if (exception != null) {
            exceptionClass = exception.getClass().getName();
            exceptionMessage = exception.getMessage();
        }
    }

    ExceptionFields(String exceptionClass, String exceptionMessage) {
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
    }

    Exception getException() {
        if (exceptionClass == null) {
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
        return new UnmappableException("Exception class: " + exceptionClass +
            "; Exception message: " + exceptionMessage);
    }

    String getExceptionClass() {
        return exceptionClass;
    }

    String getExceptionMessage() {
        return exceptionMessage;
    }
}
