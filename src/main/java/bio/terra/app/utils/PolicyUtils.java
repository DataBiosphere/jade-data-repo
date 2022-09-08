package bio.terra.app.utils;

import bio.terra.model.PolicyModel;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.SamPolicyModel;
import java.util.List;
import java.util.stream.Collectors;

public class PolicyUtils {

  public static List<PolicyModel> samToTdrPolicyModels(List<SamPolicyModel> samPolicyModels) {
    return samPolicyModels.stream()
        .map(spm -> new PolicyModel().name(spm.getName()).members(spm.getMembers()))
        .collect(Collectors.toList());
  }

  public static List<PolicyModel> resourcePolicyToPolicyModel(
      List<ResourcePolicyModel> resourcePolicyModels) {
    return resourcePolicyModels.stream()
        .map(
            rpm ->
                new PolicyModel().name(rpm.getPolicyName()).members(List.of(rpm.getPolicyEmail())))
        .collect(Collectors.toList());
  }
}
