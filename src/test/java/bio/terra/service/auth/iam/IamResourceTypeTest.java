package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.IamResourceTypeEnum;
import java.util.Objects;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class IamResourceTypeTest {
  @ParameterizedTest
  @EnumSource(IamResourceType.class)
  void testAllEnumValues(IamResourceType resource) {
    assertThat(
        "ENUM encodes and decodes",
        IamResourceType.fromEnum(IamResourceType.toIamResourceTypeEnum(resource)),
        equalTo(resource));
  }

  @ParameterizedTest
  @EnumSource(IamResourceTypeEnum.class)
  void testAllEnumValues(IamResourceTypeEnum resource) {
    assertThat(
        "ENUM encodes and decodes",
        IamResourceType.toIamResourceTypeEnum(
            Objects.requireNonNull(IamResourceType.fromEnum(resource))),
        equalTo(resource));
  }
}
