package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;
import bio.terra.service.snapshotbuilder.query.UnionQuery;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class UnionQueryTest {

  @Test
  void renderSQL() {
    var query = QueryTest.createQuery();
    var unionQuery = new UnionQuery(List.of(query, query));
    assertThat(
        unionQuery.renderSQL(SqlPlatform.SYNAPSE),
        is("SELECT t.* FROM table AS t UNION SELECT t.* FROM table AS t"));
  }

  // Suppress the warning about the constructor call inside the assertThrows lambda.
  @SuppressWarnings("ResultOfObjectAllocationIgnored")
  @Test
  void invalidUnionQuery() {
    List<Query> subQueries = List.of();
    assertThrows(IllegalArgumentException.class, () -> new UnionQuery(subQueries));
    assertThrows(IllegalArgumentException.class, () -> new UnionQuery(null));
  }
}
