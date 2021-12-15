package bio.terra.app.controller.converters;

import bio.terra.common.exception.ValidationException;
import java.util.List;
import org.springframework.core.convert.converter.Converter;

/**
 * An abstract Spring Converter to convert from strings to Enum values generated by OpenAPI. We
 * define enumerated values in OpenAPI in lower-case, but we want to be able to parse upper or
 * mixed-case values supplied by the user. We also want to be able to return pretty error messages.
 *
 * <p>This could probably be used for more than just Enums
 *
 * @param <T> A enum type to be converted.
 */
public abstract class OpenApiEnumConverter<T> implements Converter<String, T> {

  /**
   * Convert a value from String to T. Expects a lower-cased string to match enum values
   *
   * @param source Lower-cased string parameter to map to enum value
   * @return The enum value, or null if none is found
   */
  abstract T fromValue(String source);

  /** @return A specific error string for the type T. */
  abstract String errorString();

  @Override
  public T convert(String source) {
    T result = fromValue(source.toLowerCase());
    if (result == null) {
      throw new ValidationException(
          String.format("Invalid enum parameter: %s.", source), List.of(errorString()));
    }
    return result;
  }
}
