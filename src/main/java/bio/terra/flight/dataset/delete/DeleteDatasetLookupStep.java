package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.exceptions.NotFoundException;
import bio.terra.metadata.DatasetSummary;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteDatasetLookupStep implements Step {

    private DatasetDao datasetDao;
    private UUID datasetId;

    public DeleteDatasetLookupStep(DatasetDao datasetDao, UUID datasetId) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // We retrieve the dataset name before deleting anything.
        // We need to have stored the name in the working map and
        // have that serialized (meaning it has to be there at the end
        // of a step) before we delete anything. Primary data
        // dao deletes by name. Even though it may not exist by the time
        // we execute the later steps, we need to preserve the name.
        try {
            DatasetSummary summary = datasetDao.retrieveDatasetSummary(datasetId);
            FlightMap workingMap = context.getWorkingMap();
            workingMap.put("datasetName", summary.getName());
        } catch (NotFoundException nfe) {
            // we put nothing in the working map, indicating there is nothing to do
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo action for the query step
        return StepResult.getStepResultSuccess();
    }
}

