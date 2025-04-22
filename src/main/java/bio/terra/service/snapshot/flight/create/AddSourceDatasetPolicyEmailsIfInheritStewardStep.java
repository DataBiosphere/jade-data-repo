package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;

public interface AddSourceDatasetPolicyEmailsIfInheritStewardStep extends Step {
  default void addSourceDatasetPolicyEmailsIfInheritSteward(
      FlightMap workingMap, List<String> groupsToAdd, Dataset sourceDataset) {
    if (sourceDataset.isInheritSteward()) {
      Map<IamRole, String> sourceDatasetPolicyMap =
          workingMap.get(
              SnapshotWorkingMapKeys.SOURCE_DATASET_POLICY_MAP, new TypeReference<>() {});
      // Allow dataset stewards and custodians to make queries in the snapshot project.
      var result =
          sourceDatasetPolicyMap.entrySet().stream()
              .filter(entry -> DatasetService.isInheritedRole(entry.getKey()))
              .toList();
      result.stream().map(Map.Entry::getValue).forEach(groupsToAdd::add);
    }
  }
}
