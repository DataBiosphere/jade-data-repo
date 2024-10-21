package bio.terra.app.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import bio.terra.common.category.Unit;
import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class PolicyUtilsTest {

  private static final String NAME_1 = "reader";
  private static final String EMAIL_1 = "policy-1@a.com";
  private static final String NAME_2 = "steward";
  private static final String EMAIL_2 = "policy-2@b.com";
  private static final String NAME_3 = "custodian";
  private static final String EMAIL_3 = "policy-3@c.com";

  @Test
  void resourcePolicyToPolicyModel() {
    List<ResourcePolicyModel> resourcePolicyModels =
        List.of(
            new ResourcePolicyModel()
                .policyName(NAME_1)
                .policyEmail(EMAIL_1)
                .resourceId(UUID.randomUUID()),
            new ResourcePolicyModel()
                .policyName(NAME_2)
                .policyEmail(EMAIL_2)
                .resourceId(UUID.randomUUID()),
            new ResourcePolicyModel()
                .policyName(NAME_3)
                .policyEmail(EMAIL_3)
                .resourceId(UUID.randomUUID()));

    assertThat(
        PolicyUtils.resourcePolicyToPolicyModel(resourcePolicyModels),
        contains(
            new PolicyModel().name(NAME_1).addMembersItem(EMAIL_1),
            new PolicyModel().name(NAME_2).addMembersItem(EMAIL_2),
            new PolicyModel().name(NAME_3).addMembersItem(EMAIL_3)));
  }
}
