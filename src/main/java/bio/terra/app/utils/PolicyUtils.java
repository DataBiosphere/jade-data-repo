package bio.terra.app.utils;

import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.SamPolicyModel;
import java.util.List;

public class PolicyUtils {

  public static List<PolicyModel> samToTdrPolicyModels(List<SamPolicyModel> samPolicyModels) {
    return samPolicyModels.stream()
        .map(spm -> new PolicyModel().name(spm.getName()).members(spm.getMembers()))
        .toList();
  }

  public static List<PolicyModel> resourcePolicyToPolicyModel(
      List<ResourcePolicyModel> resourcePolicyModels) {
    return resourcePolicyModels.stream()
        .map(
            rpm -> new PolicyModel().name(rpm.getPolicyName()).addMembersItem(rpm.getPolicyEmail()))
        .toList();
  }
}
