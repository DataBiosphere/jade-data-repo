package bio.terra.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.dao.RecoverableDataAccessException;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public final class DaoUtils {

    private DaoUtils() {
    }

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

    public static Array createSqlUUIDArray(Connection connection, List<UUID> list) throws SQLException {
        if (list == null) {
            return null;
        }
        return connection.createArrayOf("UUID", list.toArray());
    }

    public static Array createSqlStringArray(Connection connection, List<String> list) throws SQLException {
        if (list == null) {
            return null;
        }
        return connection.createArrayOf("text", list.toArray());
    }

    public static List<UUID> getUUIDList(ResultSet rs, String column) throws SQLException {
        Array sqlArray = rs.getArray(column);
        if (sqlArray == null) {
            return null;
        }
        return Arrays.asList((UUID[]) sqlArray.getArray());
    }

    public static List<String> getStringList(ResultSet rs, String column) throws SQLException {
        Array sqlArray = rs.getArray(column);
        if (sqlArray == null) {
            return null;
        }
        return Arrays.asList((String[]) sqlArray.getArray());
    }

    // Based on Exception returned, determine if we should attempt to retry an operation/stairway step
    public static boolean retryQuery(DataAccessException dataAccessException) {
        if (ExceptionUtils.hasCause(dataAccessException, RecoverableDataAccessException.class) ||
            ExceptionUtils.hasCause(dataAccessException, TransientDataAccessException.class)) {
            return true;
        }
        return false;
    }
}
