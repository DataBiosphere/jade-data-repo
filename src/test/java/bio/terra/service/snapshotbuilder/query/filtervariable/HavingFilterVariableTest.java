package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class HavingFilterVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextTest.Contexts.class)
  void renderSQL(SqlRenderContext context) {
    HavingFilterVariable having =
        new HavingFilterVariable(BinaryFilterVariable.BinaryOperator.GREATER_THAN, 1);
    assertThat(having.renderSQL(context), is("HAVING COUNT(*) > 1"));
  }
}
