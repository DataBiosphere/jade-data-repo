package bio.terra.service.snapshot;

import java.util.List;

public interface SnapshotRequestContainer {
    Snapshot getSnapshot(SnapshotService snapshotService);

    String getName();

    List<String> getReaders();
}
