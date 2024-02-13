package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class HavingFilterVariableTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    HavingFilterVariable having =
        new HavingFilterVariable(BinaryFilterVariable.BinaryOperator.GREATER_THAN, 1);
    assertThat(having.renderSQL(CloudPlatformWrapper.of(platform)), is("HAVING COUNT(*) > 1"));
  }
}
