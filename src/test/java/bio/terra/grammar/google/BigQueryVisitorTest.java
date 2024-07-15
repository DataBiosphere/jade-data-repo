package bio.terra.grammar.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotModel;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BigQueryVisitorTest {

  @Test
  void bqTableName() {
    var snapshot = new SnapshotModel().name("name").dataProject("project");
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
    var snapshot = new SnapshotModel();
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
