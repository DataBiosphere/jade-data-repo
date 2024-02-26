package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class OrderByVariableTest {

  @NotNull
  private static FieldVariable createVariable() {
    TablePointer table = QueryTestUtils.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(table);
    var fieldVariable = new FieldVariable(new FieldPointer(table, "column"), tableVariable);
    TableVariable.generateAliases(List.of(tableVariable));
    return fieldVariable;
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLAsc(CloudPlatform platform) {
    var orderByVariable = new OrderByVariable(createVariable());
    assertThat(
        orderByVariable.renderSQL(false, CloudPlatformWrapper.of(platform)), is("t.column ASC"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLDesc(CloudPlatform platform) {
    var orderByVariable = new OrderByVariable(createVariable(), OrderByDirection.DESCENDING);
    assertThat(
        orderByVariable.renderSQL(false, CloudPlatformWrapper.of(platform)), is("t.column DESC"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLRandom(CloudPlatform platform) {
    var orderByVariable = OrderByVariable.random();
    assertThat(orderByVariable.renderSQL(true, CloudPlatformWrapper.of(platform)), is("RAND()"));
  }
}
