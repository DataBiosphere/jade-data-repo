package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class NotFilterVariableTest {

  @Test
  void renderSQL() {
    assertThat(
        new NotFilterVariable((context) -> "filter")
            .renderSQL(SqlRenderContextProvider.of(CloudPlatform.AZURE)),
        is("(NOT filter)"));
  }
}
