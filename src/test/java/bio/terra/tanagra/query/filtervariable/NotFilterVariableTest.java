package bio.terra.tanagra.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class NotFilterVariableTest {

  @Test
  void renderSQL() {
    assertThat(new NotFilterVariable(platform -> "sql").renderSQL(null), is("(NOT sql)"));
  }
}
