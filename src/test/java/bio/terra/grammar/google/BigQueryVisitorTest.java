package bio.terra.grammar.google;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import bio.terra.model.DatasetModel;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BigQueryVisitorTest {

  @Test
  void bqTableName() {
    DatasetModel dataset = new DatasetModel().name("name").dataProject("project");
    String tableName = "table";
    String fullTableName = BigQueryVisitor.bqTableName(dataset).generate(tableName);
    String expected = "`project.datarepo_name.table`";
    assertThat(
        "the expected table name has been generated.",
        fullTableName,
        Matchers.equalToCompressingWhiteSpace(expected));
  }
}
