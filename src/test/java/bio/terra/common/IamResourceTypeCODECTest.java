package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IamResourceTypeCODECTest {

  @Test
  public void testAllEnumValues() {
    for (IamResourceType resource : IamResourceType.values()) {
      assertThat(
          "CODEC encodes and decodes",
          IamResourceTypeCODEC.toIamResourceType(
              IamResourceTypeCODEC.toIamResourceTypeEnum(resource).toString()),
          equalTo(resource));
    }
  }
}
