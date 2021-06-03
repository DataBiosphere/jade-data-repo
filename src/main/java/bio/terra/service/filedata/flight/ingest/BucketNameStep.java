package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.DataLocationSelector;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class BucketNameStep implements Step {

    private final UUID datasetId;
    private final DataLocationSelector dataLocationSelector;

    public BucketNameStep(UUID datasetId, DataLocationSelector dataLocationSelector) {
        this.datasetId = datasetId;
        this.dataLocationSelector = dataLocationSelector;
    }

    @Override
    public StepResult doStep(FlightContext flightContext) {
        FlightMap workingMap = flightContext.getWorkingMap();
        BillingProfileModel billingProfile =
            workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
        String bucketName = dataLocationSelector.bucketForFile(datasetId, billingProfile);
        workingMap.put(FileMapKeys.POSSIBLE_BUCKET_NAME, bucketName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext flightContext) {
        return StepResult.getStepResultSuccess();
    }
}
