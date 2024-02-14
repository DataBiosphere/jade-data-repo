package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class TablePointerTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    var tablePointer =
        new TablePointer(
            "table", (primaryTable, tables) -> (cloudPlatform) -> "filter", null, s -> s);
    assertThat(
        tablePointer.renderSQL(CloudPlatformWrapper.of(platform)),
        is("(SELECT t.* FROM table AS t WHERE filter)"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLWithDatasetModel(CloudPlatform platform) {
    DatasetModel dataset =
        new DatasetModel().name("name").dataProject("project").cloudPlatform(platform);
    var tablePointer =
        new TablePointer(
            "table",
            (primaryTable, tables) -> (cloudPlatform) -> "filter",
            null,
            BigQueryVisitor.bqTableName(dataset));
    assertThat(
        tablePointer.renderSQL(CloudPlatformWrapper.of(dataset.getCloudPlatform())),
        is("(SELECT t.* FROM `project.datarepo_name.table` AS t WHERE filter)"));
  }
}
