package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class FieldPointerTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildVariable(CloudPlatform platform) {
    TablePointer table = QueryTestUtils.fromTableName("table");
    var fieldPointer = FieldPointer.allFields(table);
    TableVariable primaryTable = TableVariable.forPrimary(table);
    var fieldVariable = fieldPointer.buildVariable(primaryTable, null);
    TableVariable.generateAliases(List.of(primaryTable));
    assertThat(fieldVariable.renderSQL(CloudPlatformWrapper.of(platform)), is("t.*"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void buildVariableForeign(CloudPlatform platform) {
    TablePointer table = QueryTestUtils.fromTableName("table");
    var fieldPointer = FieldPointer.foreignColumn(table, "column");
    TableVariable primaryTable = TableVariable.forPrimary(table);
    List<TableVariable> tables = new ArrayList<>();
    tables.add(primaryTable);
    var fieldVariable = fieldPointer.buildVariable(primaryTable, tables);
    TableVariable.generateAliases(tables);
    assertThat(fieldVariable.renderSQL(CloudPlatformWrapper.of(platform)), is("t0.column"));
  }
}
