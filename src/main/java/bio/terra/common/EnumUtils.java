package bio.terra.common;

import java.util.EnumSet;

/** Utility methods for code that tends to be repeated in different enums */
public final class EnumUtils {

  private EnumUtils() {}

  /**
   * Returns the value of the enum where the name matches value in a case-insensitive way. Returns
   * null if name is not matched
   */
  public static <E extends Enum<E>> E valueOfLenient(Class<E> clazz, String value) {
    for (E en : EnumSet.allOf(clazz)) {
      if (en.name().equalsIgnoreCase(value)) {
        return en;
      }
    }
    return null;
  }
}
