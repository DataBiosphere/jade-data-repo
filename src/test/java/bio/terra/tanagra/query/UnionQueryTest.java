package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.underlay.DataPointer;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class UnionQueryTest {

  @Test
  void renderSQL() {
    TablePointer tablePointer =
        TablePointer.fromTableName(
            new DataPointer(DataPointer.Type.BQ_DATASET, "dataset") {
              @Override
              public String getTableSQL(String tableName) {
                return tableName;
              }

              @Override
              public String getTablePathForIndexing(String tableName) {
                return tableName;
              }

              @Override
              public Literal.DataType lookupDatatype(
                  FieldPointer fieldPointer, QueryExecutor executor) {
                return Literal.DataType.STRING;
              }
            },
            "table");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    var query =
        new Query(
            List.of(new FieldVariable(FieldPointer.allFields(tablePointer), tableVariable)),
            List.of(tableVariable),
            null,
            null,
            null,
            null,
            null);
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
