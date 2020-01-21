package bio.terra.service.snapshot;

import bio.terra.model.SnapshotProvidedIdsRequestModel;
import java.util.List;

public class SnapshotProvidedIdsRequest implements SnapshotRequestContainer  {
    private final SnapshotProvidedIdsRequestModel requestModel;

    public SnapshotProvidedIdsRequest(SnapshotProvidedIdsRequestModel requestModel) {
        this.requestModel = requestModel;
    }

    @Override
    public Snapshot getSnapshot(SnapshotService snapshotService) {
        return snapshotService.makeSnapshotFromSnapshotProvidedIdsRequest(requestModel);
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
