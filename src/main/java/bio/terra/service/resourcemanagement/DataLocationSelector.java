package bio.terra.service.resourcemanagement;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;

public interface DataLocationSelector {

    String projectIdForDataset(Dataset dataset);

    String projectIdForSnapshot(Snapshot snapshot);

    String projectIdForFile(String fileProfileId);

    String bucketForFile(String fileProfileId);
}
