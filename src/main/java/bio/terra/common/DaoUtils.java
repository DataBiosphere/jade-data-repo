package bio.terra.common;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PSQLException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
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

    /**
     * Check if the JDBC exception is due to a particular constraint violation.
     *
     * Note that the SQL code for this is hard-coded here because it's not currently included in the PSQLState enum:
     * https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/util/PSQLState.java
     * The full list of codes is on the web here: https://www.postgresql.org/docs/9.2/errcodes-appendix.html
     * There is an enhancement request pending to update their enum class to include all the codes:
     * https://github.com/pgjdbc/pgjdbc/issues/534
     *
     * @param daEx the exception to check
     * @param constraintName the name of the database constraint
     */
    private static int PSQL_STATE_UNIQUE_VIOLATION = 23505;
    public static boolean isUniqueViolationException(DataAccessException daEx, String constraintName) {
        try {
            // DataAccessException is the Spring wrapper exception for JDBC exceptions
            PSQLException psqlEx = (PSQLException) daEx.getRootCause();

            return (psqlEx != null
                && psqlEx.getSQLState().equals(Integer.toString(PSQL_STATE_UNIQUE_VIOLATION))
                && psqlEx.getServerErrorMessage().getConstraint().equals(constraintName));
        } catch (Exception ex) {
            return false;
        }
    }
}
