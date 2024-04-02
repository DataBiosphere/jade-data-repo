package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.QueryTest;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class SubQueryFilterVariableTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    var subQuery = QueryTest.createQuery();

    var fieldPointer = new FieldPointer(null, "field");
    var tableVariable = TableVariable.forPrimary(QueryTestUtils.fromTableName("x"));
    TableVariable.generateAliases(List.of(tableVariable));
    var fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    var filter = SubQueryFilterVariable.in(fieldVariable, subQuery);
    assertThat(
        filter.renderSQL(CloudPlatformWrapper.of(platform)),
        is("x.field IN (SELECT t.* FROM table AS t)"));
  }
}
