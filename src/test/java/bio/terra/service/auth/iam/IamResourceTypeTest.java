package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.IamResourceTypeEnum;
import java.util.Objects;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IamResourceTypeTest {

  @Test
  public void testAllEnumValues() {
    for (IamResourceType resource : IamResourceType.values()) {
      assertThat(
          "ENUM encodes and decodes",
          IamResourceType.fromEnum(resource.getIamResourceTypeEnum()),
          equalTo(resource));
    }

    for (IamResourceTypeEnum resource : IamResourceTypeEnum.values()) {
      assertThat(
          "ENUM encodes and decodes",
          IamResourceType.toIamResourceTypeEnum(
              Objects.requireNonNull(IamResourceType.fromEnum(resource))),
          equalTo(resource));
    }
  }
}
