package bio.terra.dao;

public final class DaoUtils {

    private DaoUtils() {}

    public static String orderByClause(String sort, String direction) {
        if (sort == null || sort.isEmpty() || direction == null || direction.isEmpty()) {
            return "";
        }
        return new StringBuilder(" ORDER BY ")
            .append(sort).append(" ")
            .append(direction).append(" ")
            .toString();
    }

    public static String whereClause(String filter) {
        return whereClause(filter, false);
    }

    public static String whereClause(String filter, boolean isAnded) {
        if (filter == null || filter.isEmpty()) {
            return "";
        }
        StringBuilder clause = new StringBuilder();
        if (isAnded) {
            clause.append(" AND ");
        } else {
            clause.append(" WHERE ");
        }
        return clause.append(" (name ILIKE :filter OR description ILIKE :filter) ").toString();
    }

    public static String escapeFilter(String filter) {
        StringBuilder builder = new StringBuilder("%");
        for (char c : filter.toCharArray()) {
            if (c == '_' || c == '%') {
                builder.append('\\');
            }
            builder.append(c);
        }
        return builder.append('%').toString();
    }
}
