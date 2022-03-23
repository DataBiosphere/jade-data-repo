package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
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
}
