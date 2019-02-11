package bio.terra.validation;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

public final class Utils {

    private static final String VALID_NAME_REGEX = "[a-zA-Z0-9_]{1,63}";

    private Utils() {}

    public static <T> boolean hasDuplicates(List<T> list) {
        HashSet<T> seen = new HashSet<>();
        for (T item : list) {
            if (seen.contains(item)) {
                return true;
            }
            seen.add(item);
        }
        return false;
    }

    public static boolean isValidName(String name) {
        return Pattern.matches(VALID_NAME_REGEX, name);
    }
}
