package bio.terra.app.utils;

/**
 * Utility methods for working with the Terra Interoperability Model (TIM)
 */
public final class TimUtils {
    private TimUtils() { }

    private static final String CAPITAL = "a__";
    private static final String COLON = "c__";
    private static final String PERIOD = "p__";
    private static final String PREFIX = "tim__";

    /**
     * Encode a TIM name for ElasticSearch or BigQuery use.
     *
     * @param s the TIM name
     * @return an encoded name
     */
    public static String encode(String s) {
        StringBuilder result = new StringBuilder(PREFIX);
        for (char c : s.toCharArray()) {
            if (Character.isUpperCase(c)) {
                result.append(CAPITAL);
                result.append(Character.toLowerCase(c));
            } else if (c == ':') {
                result.append(COLON);
            } else if (c == '.') {
                result.append(PERIOD);
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private static String doDecode(String s) {
        String t = s.substring(PREFIX.length())
            .replaceAll(COLON, ":")
            .replaceAll(PERIOD, ".");
        int len = CAPITAL.length();
        int index;
        while ((index = t.indexOf(CAPITAL)) != -1) {
            t = t.substring(0, index) + Character.toUpperCase(t.charAt(index + len)) + t.substring(index + len + 1);
        }
        return t;
    }

    /**
     * Give a string, return true if it's likely that this was encoded and requires decoding.
     *
     * @param s the string
     * @return true if it's likely to be encoded
     */
    private static boolean shouldDecode(String s) {
        // Only decode names that start with an encoded TIM prefix.
        return s.startsWith(PREFIX);
    }

    /**
     * Decode an encoded TIM name and return its original name.
     *
     * @param s the encoded name
     * @return the decoded TIM name
     */
    public static String decode(String s) {
        if (shouldDecode(s)) {
            return doDecode(s);
        }
        return s;
    }
}
