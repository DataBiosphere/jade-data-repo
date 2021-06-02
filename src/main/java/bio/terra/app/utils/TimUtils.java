package bio.terra.app.utils;

/**
 * Utility methods for working with the Terra Interoperability Model (TIM)
 */
public final class TimUtils {
    private TimUtils() { }

    private static final String CAPITOL = "a__";
    private static final String COLON = "c__";
    private static final String PERIOD = "p__";

    /**
     * Encode a TIM name for ElasticSearch or BigQuery use.
     *
     * @param s the TIM name
     * @return an encoded name
     */
    public static String encode(String s) {
        StringBuilder result = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (Character.isUpperCase(c)) {
                result.append(CAPITOL);
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
        String t = s.replaceAll(COLON, ":").replaceAll(PERIOD, ".");
        int len = CAPITOL.length();
        int index;
        while ((index = t.indexOf(CAPITOL)) != -1) {
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
        // Search for two code strings that all names will have at least one of.
        return s.contains(COLON) && s.contains(CAPITOL);
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
