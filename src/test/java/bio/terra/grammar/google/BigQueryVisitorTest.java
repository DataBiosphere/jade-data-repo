package bio.terra.grammar.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BigQueryVisitorTest {

  @Test
  void bqTableName() {
    var dataset = new DatasetSummaryModel().name("name").dataProject("project");
    var snapshot = new SnapshotModel().addSourceItem(new SnapshotSourceModel().dataset(dataset));
    String tableName = "table";
    String fullTableName = BigQueryVisitor.bqSnapshotTableName(snapshot).generate(tableName);
    String expected = "`project.name.table`";
    assertThat(
        "the expected table name has been generated.",
        fullTableName,
        Matchers.equalToCompressingWhiteSpace(expected));
  }

  @Test
  void bqTableNameNull() {
    var dataset = new DatasetSummaryModel();
    var snapshot = new SnapshotModel().addSourceItem(new SnapshotSourceModel().dataset(dataset));
    String tableName = null;
    String fullTableName = BigQueryVisitor.bqSnapshotTableName(snapshot).generate(tableName);
    String expected = "`null.null.null`";
    assertThat(
        "the expected table name has been generated.",
        fullTableName,
        Matchers.equalToCompressingWhiteSpace(expected));
  }

  @Test
  void bqTableNameNullModel() {
    TableNameGenerator generator = BigQueryVisitor.bqSnapshotTableName(null);
    assertThrows(NullPointerException.class, () -> generator.generate(null));
  }
}
