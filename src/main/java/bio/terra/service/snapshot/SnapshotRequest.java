package bio.terra.service.snapshot;

import bio.terra.model.SnapshotRequestModel;

import java.util.List;

public class SnapshotRequest implements SnapshotRequestContainer  {
    private final SnapshotRequestModel requestModel;

    public SnapshotRequest(SnapshotRequestModel requestModel) {
        this.requestModel = requestModel;
    }

    @Override
    public Snapshot getSnapshot(SnapshotService snapshotService) {
        return snapshotService.makeSnapshotFromSnapshotRequest(requestModel);
    }

    @Override
    public String getName() {
        return requestModel.getName();
    }

    @Override
    public List<String> getReaders() {
        return requestModel.getReaders();
    }
}
