package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class TablePointerTest {

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderSQL(SqlRenderContext context) {
    var tablePointer =
        new TablePointer("table", (primaryTable, tables) -> (cloudPlatform) -> "filter", null);
    assertThat(tablePointer.renderSQL(context), is("(SELECT t.* FROM table AS t WHERE filter)"));
  }

  @Test
  void renderSQLWithDatasetModel() {
    var tablePointer =
        new TablePointer("table", (primaryTable, tables) -> (cloudPlatform) -> "filter", null);
    var generatedName = "generated-table-name";
    assertThat(
        tablePointer.renderSQL(new SqlRenderContext(tableName -> generatedName, null)),
        is("(SELECT t.* FROM %s AS t WHERE filter)".formatted(generatedName)));
  }
}
