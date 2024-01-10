package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class FieldPointerTest {

  @Test
  void buildVariable() {
    TablePointer table = new TablePointer("table", null, null, Function.identity());
    var fieldPointer = FieldPointer.allFields(table);
    TableVariable primaryTable = TableVariable.forPrimary(table);
    var fieldVariable = fieldPointer.buildVariable(primaryTable, null);
    TableVariable.generateAliases(List.of(primaryTable));
    assertThat(fieldVariable.renderSQL(), is("t.*"));
  }

  @Test
  void buildVariableForeign() {
    TablePointer table = new TablePointer("table", null, null, Function.identity());
    var fieldPointer = FieldPointer.foreignColumn(table, "column");
    TableVariable primaryTable = TableVariable.forPrimary(table);
    List<TableVariable> tables = new ArrayList<>();
    tables.add(primaryTable);
    var fieldVariable = fieldPointer.buildVariable(primaryTable, tables);
    TableVariable.generateAliases(tables);
    assertThat(fieldVariable.renderSQL(), is("t0.column"));
  }
}
