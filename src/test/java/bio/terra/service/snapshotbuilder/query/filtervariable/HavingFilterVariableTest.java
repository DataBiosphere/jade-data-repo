package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class HavingFilterVariableTest {

  @Test
  void renderSQL() {
    HavingFilterVariable having =
        new HavingFilterVariable(BinaryFilterVariable.BinaryOperator.GREATER_THAN, 1);
    assertThat(having.renderSQL(SqlPlatform.BIGQUERY), is("HAVING COUNT(*) > 1"));
  }
}
