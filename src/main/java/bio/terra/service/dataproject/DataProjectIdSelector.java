package bio.terra.service.dataproject;

import bio.terra.metadata.Snapshot;
import bio.terra.metadata.Dataset;

public interface DataProjectIdSelector {

    String projectIdForDataset(Dataset dataset);

    String projectIdForSnapshot(Snapshot snapshot);
}
