package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.QueryTest;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class SubQueryFilterVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQL(SqlRenderContext context) {
    var subQuery = QueryTest.createQuery();

    var fieldPointer = new FieldPointer(null, "field");
    var tableVariable = SourceVariable.forPrimary(TablePointer.fromTableName("x"));
    var fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    var filter = SubQueryFilterVariable.in(fieldVariable, subQuery);
    assertThat(filter.renderSQL(context), is("x.field IN (SELECT t.* FROM table AS t)"));
  }
}
