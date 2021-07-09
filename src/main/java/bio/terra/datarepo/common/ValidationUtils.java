package bio.terra.datarepo.common;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

public final class ValidationUtils {

  // pattern taken from https://stackoverflow.com/questions/8204680/java-regex-email
  private static final Pattern VALID_EMAIL_REGEX =
      Pattern.compile(
          "[a-z0-9!#$%&'*+/=?^_`{|}~-]+"
              + "(?:.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?",
          Pattern.CASE_INSENSITIVE);

  // path needs to start with a leading forward slash
  // TODO: This validation check is NOT required by the underlying code. The filesystem dao is
  // more forgiving. Should we enforce this or fix it up, as is done in filesystem.
  private static final String VALID_PATH = "/.*";

  private ValidationUtils() {}

  public static <T> boolean hasDuplicates(List<T> list) {
    return !(list.size() == new HashSet(list).size());
  }

  public static boolean isValidDescription(String name) {
    return name.length() < 2048;
  }

  public static boolean isValidEmail(String email) {
    return VALID_EMAIL_REGEX.matcher(email).matches();
  }

  public static boolean isValidPath(String path) {
    return Pattern.matches(VALID_PATH, path);
  }

  public static boolean isValidUuid(String uuid) {
    return convertToUuid(uuid).isPresent();
  }

  public static Optional<UUID> convertToUuid(String uuid) {
    try {
      return Optional.of(UUID.fromString(uuid));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
