package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class FieldPointerTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildVariable(SqlRenderContext context) {
    TablePointer table = TablePointer.fromTableName("table");
    var fieldPointer = FieldPointer.allFields(table);
    SourceVariable primaryTable = SourceVariable.forPrimary(table);
    var fieldVariable = fieldPointer.buildVariable(primaryTable, null);
    assertThat(fieldVariable.renderSQL(context), is("t.*"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildVariableForeign(SqlRenderContext context) {
    TablePointer table = TablePointer.fromTableName("table");
    var fieldPointer = FieldPointer.foreignColumn(table, "column");
    SourceVariable primaryTable = SourceVariable.forPrimary(table);
    List<SourceVariable> tables = new ArrayList<>();
    tables.add(primaryTable);
    var fieldVariable = fieldPointer.buildVariable(primaryTable, tables);
    assertThat(fieldVariable.renderSQL(context), is("t.column"));
  }
}
