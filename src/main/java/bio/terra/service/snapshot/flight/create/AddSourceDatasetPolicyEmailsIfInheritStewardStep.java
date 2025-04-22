package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;

public abstract class AddSourceDatasetPolicyEmailsIfInheritStewardStep {
  List<String> addSourceDatasetPolicyEmailsIfInheritSteward(
      FlightMap workingMap, Dataset sourceDataset) {
    if (!sourceDataset.isInheritSteward()) {
      return List.of();
    }
    Map<IamRole, String> sourceDatasetPolicyMap =
        workingMap.get(SnapshotWorkingMapKeys.SOURCE_DATASET_POLICY_MAP, new TypeReference<>() {});
    // Allow dataset stewards and custodians to make queries in the snapshot project.
    return sourceDatasetPolicyMap.entrySet().stream()
        .filter(entry -> DatasetService.isInheritedRole(entry.getKey()))
        .map(Map.Entry::getValue)
        .toList();
  }
}
