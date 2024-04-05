package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class FunctionFilterVariableTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    TableVariable table = TableVariable.forPrimary(QueryTestUtils.fromTableName("table"));
    var filterVariable =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.IN,
            new FieldVariable(new FieldPointer(null, "column"), table),
            new Literal("value1"),
            new Literal("value2"));
    assertThat(
        filterVariable.renderSQL(QueryTestUtils.createContext(platform)),
        is("t.column IN ('value1','value2')"));
  }
}
