package bio.terra.dao;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.List;
import java.util.UUID;

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

    public static void addFilterClause(String filter, MapSqlParameterSource params, List<String> clauses) {
        if (!StringUtils.isEmpty(filter)) {
            params.addValue("filter", DaoUtils.escapeFilter(filter));
            clauses.add(" (name ILIKE :filter OR description ILIKE :filter) ");
        }
    }

    public static void addAuthzIdsClause(List<UUID> authzIds, MapSqlParameterSource params, List<String> clauses) {
        params.addValue("idlist", authzIds);
        clauses.add(" id in (:idlist) ");
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
