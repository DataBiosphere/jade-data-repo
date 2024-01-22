package bio.terra.grammar.azure;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SynapseVisitorTest {

  @Test
  void azureTableName() {
    String datasetSource = "source_dataset_data_source_0";
    String tableName = "table";
    String fullTableName = SynapseVisitor.azureTableName(datasetSource).generate(tableName);
    String expected =
        """
          (SELECT * FROM
          OPENROWSET(
            BULK 'metadata/parquet/table/*/*.parquet',
            DATA_SOURCE = 'source_dataset_data_source_0',
            FORMAT = 'parquet') AS inner_alias110115821)
          """;
    assertThat(
        "the expected table name and alias have been generated.",
        fullTableName,
        Matchers.equalToCompressingWhiteSpace(expected));
  }
}
