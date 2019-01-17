package bio.terra.stairway;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A wrapper around SQL prepared statements that handles named parameters.
 * It implements AutoCloseable so it can be used in try-with-resources.
 * The constructor parses a SQL string with embedded  <code>:name</code>
 * parameters. The name string is terminated with one of a set of characters:
 * space, comma, close paren.
 *
 * The class maintains a map of the name to the parameter index and replaces the
 * <code>:name</code> with a <code>?</code> parameter marker.
 *
 * The class provides a subset of the setters used to give values to parameters in the
 * prepared statement. It handles only the types needed by Stairway, but can easily be
 * extended for more types.
 *
 * Because we are dynamically processing the SQL string to map parameter names to values
 * we violate the FindBugs rule about generating prepared statements from nonconstant Strings.
 * Therefore, we put the FindBugs exception in this class.
 */
@SuppressFBWarnings(
        value = "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING",
        justification = "Can't handle named parameters without manipulating the string")
public class NamedParameterPreparedStatement implements AutoCloseable {
    private PreparedStatement preparedStatement;  // prepared statement object
    private Map<String, Integer> nameIndexMap;    // mapping between parameter names and indexes

    /**
     * Construct a prepared statement, extracts named parameters and inserts parameter
     * markers (? characters). It prepares the resulting string.
     *
     * @param connection SQL connection in which to prepare the statement
     * @param sql statement to prepare
     * @throws SQLException
     */
    public NamedParameterPreparedStatement(Connection connection, String sql) throws SQLException {
        nameIndexMap = new HashMap<>();
        final String nameTerminators = " ,)"; // characters that will terminate a parameter name.

        int index = 1;
        int pos;
        while ((pos = StringUtils.indexOf(sql, ":")) != -1) {
            int end = StringUtils.indexOfAny(sql.substring(pos), nameTerminators);
            if (end == -1) {
                end = sql.length();
            } else {
                end += pos;
            }
            String name = sql.substring(pos + 1, end);
            nameIndexMap.put(name, index);
            index++;

            sql = sql.substring(0, pos) + "?" + sql.substring(end);
        }
        preparedStatement = connection.prepareStatement(sql);
    }

    /**
     * Support AutoCloseable by providing a close method.
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException {
        preparedStatement.close();
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    private int getIndex(String name) {
        Integer index = nameIndexMap.get(name);
        if (index == null) {
            throw new IllegalArgumentException("Parameter name '" + name +
                    "' is not a valid parameter in the prepared statement");
        }
        return index;
    }

    // Type-specific parameter setters
    // I included all of the ones Stairway needs. If we need other datatypes,
    // they are trivial to add here.
    public void setBoolean(String name, boolean value) throws SQLException {
        preparedStatement.setBoolean(getIndex(name), value);
    }

    public void setInt(String name, int value) throws SQLException {
        preparedStatement.setInt(getIndex(name), value);
    }

    public void setString(String name, String value) throws SQLException {
        preparedStatement.setString(getIndex(name), value);
    }







}
