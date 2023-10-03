package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.exception.SystemException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class ColumnHeaderSchemaTest {

  @Test
  void getIndex() {
    List<ColumnSchema> schemas = List.of(
        new ColumnSchema("a", CellValue.SQLDataType.INT64),
        new ColumnSchema("b", CellValue.SQLDataType.STRING),
        new ColumnSchema("c", CellValue.SQLDataType.BOOLEAN));

    assertThat(new ColumnHeaderSchema(schemas).getIndex(schemas.get(2).columnName()), is(2));
  }

  @Test
  void getIndexNotFound() {
    var columnHeaderSchema =
        new ColumnHeaderSchema(List.of(new ColumnSchema("a", CellValue.SQLDataType.INT64)));

    String name = "dddd";
    var thrown = assertThrows(SystemException.class, () -> columnHeaderSchema.getIndex(name));
    assertThat(thrown.getMessage(), containsString(name));
  }

  @Test
  void columnSchemas() {
    ColumnSchema a = new ColumnSchema("a", CellValue.SQLDataType.STRING);
    ColumnSchema z = new ColumnSchema("a", CellValue.SQLDataType.STRING);
    var columnHeaderSchema = new ColumnHeaderSchema(List.of(z, a));
    assertThat(columnHeaderSchema.columnSchemas(), is(List.of(a, z)));
  }
}
