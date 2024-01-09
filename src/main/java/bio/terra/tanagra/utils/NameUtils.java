package bio.terra.tanagra.utils;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;

/**
 * Utilities for checking Tanagra names.
 *
 * <p>Some names traverse Tanagra from HTTP requests to SQL generation. For these names, it's
 * important that they are relatively friendly and simple.
 */
final class NameUtils {
  private NameUtils() {}

  // Start with simple lower case letters, numbers, and underscores of less than 32 characters.
  // We could add more valid characters, or allow different characters for different places, but
  // this is a simple starting point.
  private static final String NAME_REGEX = "^[a-z][a-zA-Z0-9_]{0,41}$";
  private static final Pattern NAME_VALIDATOR = Pattern.compile(NAME_REGEX);

  // User-defined attributes should not use this prefix. It is reserved for Tanagra-generated
  // attributes (e.g. the t_path_xx attribute that is added for an entity with a hierarchy on
  // attribute xx).
  private static final String TANAGRA_RESERVED_PREFIX = "t_";

  /**
   * Check that the {@code name} matches the {@link #NAME_REGEX}, or else throws an
   * IllegalArgumentException.
   */
  public static void checkName(String name, String fieldName) {
    Preconditions.checkArgument(
        NAME_VALIDATOR.matcher(name).matches(),
        "%s must match regex '%s' but name was '%s'",
        fieldName,
        NAME_REGEX,
        name);
  }

  /**
   * Check that the {@code name} does not start with the {@link #TANAGRA_RESERVED_PREFIX}, or else
   * throws an IllegalArgumentException.
   */
  public static void checkNameForReservedPrefix(String name, String fieldName) {
    Preconditions.checkArgument(
        !name.startsWith(TANAGRA_RESERVED_PREFIX),
        "%s cannot start with the reserved prefix '%s', but name was '%s'",
        fieldName,
        TANAGRA_RESERVED_PREFIX,
        name);
  }
}
