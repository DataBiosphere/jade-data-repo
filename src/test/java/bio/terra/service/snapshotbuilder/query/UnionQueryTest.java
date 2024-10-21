package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class UnionQueryTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQL(SqlRenderContext context) {
    var query = QueryTest.createQuery();
    var unionQuery = new UnionQuery(List.of(query, query));
    assertThat(
        unionQuery.renderSQL(context),
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
