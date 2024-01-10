package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import java.util.function.Function;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class TablePointerTest {

  @Test
  void renderSQL() {
    var tablePointer =
        new TablePointer(
            "table", (primaryTable, tables) -> () -> "filter", null, Function.identity());
    assertThat(tablePointer.renderSQL(), is("(SELECT t.* FROM table AS t WHERE filter)"));
  }

  @Test
  void renderSQLWithDatasetModel() {
    DatasetModel dataset =
        new DatasetModel().name("name").dataProject("project").cloudPlatform(CloudPlatform.GCP);
    var tablePointer =
        new TablePointer(
            "table",
            (primaryTable, tables) -> () -> "filter",
            null,
            QueryTest.generateTableName(dataset));
    assertThat(
        tablePointer.renderSQL(),
        is("(SELECT t.* FROM `project.datarepo_name.table` AS t WHERE filter)"));
  }
}
