package bio.terra.app.utils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * Encode a TIM name for ElasticSearch or BigQuery use. If the input string doesn't need
     * encoding, the original string will be returned.
     *
     * @param s the TIM name
     * @return an encoded name, or the original if no encoding is needed
     */
    public static String encode(String s) {
        StringBuilder result = new StringBuilder(PREFIX);
        // Keep track of encodings we use. Only colon or period encodings are required.
        var needsEncoding = false;
        for (char c : s.toCharArray()) {
            if (Character.isUpperCase(c)) {
                result.append(CAPITAL);
                result.append(Character.toLowerCase(c));
            } else if (c == ':') {
                result.append(COLON);
                needsEncoding = true;
            } else if (c == '.') {
                result.append(PERIOD);
                needsEncoding = true;
            } else {
                result.append(c);
            }
        }
        if (!needsEncoding) {
            // If no encoding was needed, return the original string.
            return s;
        }
        return result.toString();
    }

    /**
     * Encode the AS columns of a SQL query into corresponding TIM property names
     *
     * @param sql the SQL string containing AS columns to encode
     * @param columnReplacements a map which stores column names as keys and TIM property names as values
     * @return a modified SQL string containing encoded TIM property names
     */
    public static String encodeSqlColumns(String sql, Map<String, String> columnReplacements) {
        Pattern regex = Pattern.compile("( [aA][sS] )(\\w+)");
        Matcher matches = regex.matcher(sql);
        StringBuilder sb = new StringBuilder(sql.length());
        while (matches.find()) {
            String replacement = columnReplacements.get(matches.group(2));
            if (replacement != null) {
                matches.appendReplacement(sb, matches.group(1) + TimUtils.encode(replacement));
            }
        }
        matches.appendTail(sb);
        return sb.toString();
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
