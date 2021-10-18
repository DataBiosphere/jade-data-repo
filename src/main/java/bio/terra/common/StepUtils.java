package bio.terra.common;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepUtils {

  private static final Logger logger = LoggerFactory.getLogger(StepUtils.class);

  public static String keyFromField(Field field) {
    // TODO: add support for name overrides.
    return field.getName();
  }

  public static void readInputs(Step step, FlightContext context) {
    for (Class<?> clazz = step.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      for (Field field : clazz.getDeclaredFields()) {
        if (field.isAnnotationPresent(StepInput.class)) {
          String key = keyFromField(field);
          if (context.getInputParameters().containsKey(key)) {
            setField(step, context.getInputParameters(), field, key);
          } else if (context.getWorkingMap().containsKey(key)) {
            setField(step, context.getWorkingMap(), field, key);
          } else {
            throw new RuntimeException("No flight value found for key '" + key + "'");
          }
        }
      }
    }
  }

  private static void setField(Step step, FlightMap map, Field field, String key) {
    field.setAccessible(true);
    try {
      field.set(step, map.get(key, field.getType()));
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static void writeOutputs(Step step, FlightContext context) {
    for (Class<?> clazz = step.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      for (Field field : clazz.getDeclaredFields()) {
        if (field.isAnnotationPresent(StepOutput.class)) {
          field.setAccessible(true);
          try {
            final Object value = field.get(step);
            if (value == null) {
              continue;
            }
            context.getWorkingMap().put(keyFromField(field), value);
            logger.info(
                "Step {} writing '{}' to key '{}'",
                step.getClass().getSimpleName(),
                value,
                keyFromField(field));
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
