package bio.terra.resourcemanagement;

import bio.terra.dataset.Dataset;
import bio.terra.snapshot.Snapshot;

public interface DataLocationSelector {

    String projectIdForDataset(Dataset dataset);

    String projectIdForSnapshot(Snapshot snapshot);

    String projectIdForFile(String fileProfileId);

    String bucketForFile(String fileProfileId);
}
