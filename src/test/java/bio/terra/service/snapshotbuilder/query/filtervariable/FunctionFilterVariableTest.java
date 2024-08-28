package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class FunctionFilterVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQL(SqlRenderContext context) {
    SourceVariable table = SourceVariable.forPrimary(TablePointer.fromTableName("table"));
    var filterVariable =
        FunctionFilterVariable.in(
            new FieldVariable(new FieldPointer(null, "column"), table),
            List.of(new Literal("value1"), new Literal("value2")));
    assertThat(filterVariable.renderSQL(context), is("t.column IN ('value1','value2')"));
  }
}
