package bio.terra.validation;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

public final class ValidationUtils {

    private static final String VALID_NAME_REGEX = "[a-zA-Z0-9_]{1,63}";

    private ValidationUtils() {}

    public static <T> boolean hasDuplicates(List<T> list) {
        return !(list.size() == new HashSet(list).size());
    }

    public static boolean isValidName(String name) {
        return Pattern.matches(VALID_NAME_REGEX, name);
    }
}
