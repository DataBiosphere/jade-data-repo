package bio.terra.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DRSPassportRequestModelTest {

  @Test
  void testDRSPassportRequestModelDefault() {
    var modelExpandDefault = new DRSPassportRequestModel();
    var modelExpandFalse = new DRSPassportRequestModel().expand(false);

    assertThat(modelExpandDefault, equalTo(modelExpandFalse));
  }
}
