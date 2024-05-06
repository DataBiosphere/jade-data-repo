package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public abstract class DefaultByQueryStep implements Step {

  SnapshotRequestModel getByQueryRequestModel(FlightContext context) {
    return context
        .getInputParameters()
        .get(JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);
  }
}
