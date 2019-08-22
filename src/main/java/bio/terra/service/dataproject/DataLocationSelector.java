package bio.terra.service.dataproject;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.Snapshot;

public interface DataLocationSelector {

    String projectIdForDataset(Dataset dataset);

    String projectIdForSnapshot(Snapshot snapshot);

    String projectIdForFile(String fileProfileId);

    String bucketForFile(String fileProfileId);
}
