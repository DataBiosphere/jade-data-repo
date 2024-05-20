package bio.terra.common;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

// Suppress sonar warnings about reflection API usage. Reflection APIs must be used to read and
// write fields in a Step.
@SuppressWarnings({"java:S3011"})
public class StepUtils {

  public static class MissingStepInputException extends RuntimeException {
    public MissingStepInputException(String key) {
      super("No flight value found for StepInput key '" + key + "'");
    }
  }

  public static class IllegalSetException extends RuntimeException {
    public IllegalSetException(Throwable cause) {
      super(cause);
    }
  }

  public static class IllegalGetException extends RuntimeException {
    public IllegalGetException(Throwable cause) {
      super(cause);
    }
  }

  private StepUtils() {}

  public static String keyFromField(Field field) {
    var input = field.getAnnotation(StepInput.class);
    if (input != null && !input.value().isEmpty()) {
      return input.value();
    }
    var output = field.getAnnotation(StepOutput.class);
    if (output != null && !output.value().isEmpty()) {
      return output.value();
    }
    return field.getName();
  }

  private static List<Field> collect(Step step, Class<? extends Annotation> annotation) {
    List<Field> inputs = new ArrayList<>();
    for (Class<?> clazz = step.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      for (Field field : clazz.getDeclaredFields()) {
        if (field.isAnnotationPresent(annotation)) {
          inputs.add(field);
        }
      }
    }
    return inputs;
  }

  public static void readInputs(Step step, FlightContext context) throws MissingStepInputException {
    collect(step, StepInput.class)
        .forEach(
            field -> {
              String key = keyFromField(field);
              if (context.getInputParameters().containsKey(key)) {
                setField(step, context.getInputParameters(), field, key);
              } else if (context.getWorkingMap().containsKey(key)) {
                setField(step, context.getWorkingMap(), field, key);
              } else if (!field.isAnnotationPresent(StepOutput.class)) {
                // If the field is only used as an input, report an error if there's no value for
                // it.
                throw new MissingStepInputException(key);
              }
            });
  }

  private static void setField(Step step, FlightMap map, Field field, String key) {
    field.setAccessible(true);
    try {
      field.set(step, map.get(key, field.getType()));
    } catch (IllegalAccessException e) {
      throw new IllegalSetException(e);
    }
  }

  public static void writeOutputs(Step step, FlightContext context) {
    collect(step, StepOutput.class)
        .forEach(
            field -> {
              field.setAccessible(true);
              final Object value;
              try {
                value = field.get(step);
              } catch (IllegalAccessException e) {
                throw new IllegalGetException(e);
              }
              // An unset output can occur if an exception is thrown inside the run() operation.
              if (value != null) {
                context.getWorkingMap().put(keyFromField(field), value);
              }
            });
  }

  public record StepData(List<String> inputs, List<String> outputs) {}

  static StepData getStepData(Step step) {
    return new StepData(
        collect(step, StepInput.class).stream().map(StepUtils::keyFromField).toList(),
        collect(step, StepOutput.class).stream().map(StepUtils::keyFromField).toList());
  }

  // Using inputs and outputs, generate a flow analysis report
  public static List<StepData> generateFlowAnalysisReport(Flight flight) {
    return flight.getSteps().stream().map(StepUtils::getStepData).toList();
  }
}
