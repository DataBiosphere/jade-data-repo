package bio.terra.validation;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

public final class ValidationUtils {

    private static final String VALID_NAME_REGEX = "[a-zA-Z0-9_]{1,63}";

    private static final Pattern VALID_EMAIL_REGEX = Pattern.compile("[a-z0-9!#$%&'*+/=?^_`{|}~-]+" +
        "(?:.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?",
        Pattern.CASE_INSENSITIVE);

    private ValidationUtils() {}

    public static <T> boolean hasDuplicates(List<T> list) {
        return !(list.size() == new HashSet(list).size());
    }

    public static boolean isValidName(String name) {
        return Pattern.matches(VALID_NAME_REGEX, name);
    }

    public static boolean isValidDescription(String name) {
        return name.length() < 2048;
    }

    public static boolean isValidEmail(String email) { return VALID_EMAIL_REGEX.matcher(email).matches(); }

}
