package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class TablePointerTest {

  @Test
  void renderSQL() {
    var tablePointer =
        new TablePointer(
            "table", (primaryTable, tables) -> (FilterVariable) platform -> "filter", null);
    assertThat(tablePointer.renderSQL(null), is("(SELECT t.* FROM table AS t WHERE filter)"));
  }
}
