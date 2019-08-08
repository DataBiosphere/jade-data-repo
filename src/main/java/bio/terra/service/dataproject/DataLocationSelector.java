package bio.terra.service.dataproject;

import bio.terra.metadata.FSFile;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.Dataset;

public interface DataLocationSelector {

    String projectIdForDataset(Dataset dataset);

    String projectIdForSnapshot(Snapshot snapshot);

    String projectIdForFile(FSFile fsFile);

    String bucketForFile(FSFile fsFile);
}
