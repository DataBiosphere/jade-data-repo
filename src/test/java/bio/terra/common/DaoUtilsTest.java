package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class DaoUtilsTest {

  @Test
  public void testOrderByClause() {
    assertThat(
        "order by clause looks correct",
        DaoUtils.orderByClause(EnumerateSortByParam.NAME, SqlSortDirection.ASC, "foo"),
        equalTo(" ORDER BY foo.name asc "));

    assertThat(
        "default order by clause looks correct",
        DaoUtils.orderByClause(null, null, "foo"),
        equalTo(" ORDER BY foo.created_date desc "));
  }

  @Test
  public void testCreateSqlStringArray() throws SQLException {
    Connection connection = mock(Connection.class);

    // A null input list is converted to an empty array
    DaoUtils.createSqlStringArray(connection, null);
    verify(connection).createArrayOf("text", new Object[] {});

    // A non-null list is converted to an array
    List<String> list = List.of("a", "b", "c");
    DaoUtils.createSqlStringArray(connection, list);
    verify(connection).createArrayOf("text", list.toArray());
  }

  @Test
  public void testGetStringList() throws SQLException {
    ResultSet rs = mock(ResultSet.class);

    String nullColumn = "nullColumn";
    when(rs.getArray(nullColumn)).thenReturn(null);
    assertThat(
        "Null column result returns empty list", DaoUtils.getStringList(rs, nullColumn), empty());

    Array sqlEmptyArray = mock(Array.class);
    when(sqlEmptyArray.getArray()).thenReturn(new String[] {});
    String emptyColumn = "emptyColumn";
    when(rs.getArray(emptyColumn)).thenReturn(sqlEmptyArray);
    assertThat(
        "Empty array result returns empty list", DaoUtils.getStringList(rs, emptyColumn), empty());

    var array = new String[] {"a", "b", "c"};
    Array sqlArray = mock(Array.class);
    when(sqlArray.getArray()).thenReturn(array);
    String column = "column";
    when(rs.getArray(column)).thenReturn(sqlArray);
    assertThat(
        "Array result converted to list", DaoUtils.getStringList(rs, column), contains(array));
  }
}
