package bio.terra.service.dataset.flight;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidLockArgumentException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

import java.util.UUID;

// I was very tempted to call this LockStep...
public abstract class LockDatasetBaseStep implements Step {

    private DatasetDao datasetDao;
    private String datasetName;

    public LockDatasetBaseStep(DatasetDao datasetDao, String datasetName) {
        this.datasetDao = datasetDao;
        this.datasetName = datasetName;
    }

    public LockDatasetBaseStep(DatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    public DatasetDao getDatasetDao() {
        return datasetDao;
    }

    public String getDatasetName() {
        return datasetName;
    }

    protected String getDatasetName(FlightContext context) {
        if (datasetName != null) {
            return datasetName;
        }
        String datasetId = context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        if (datasetId == null) {
            throw new InvalidLockArgumentException("Must pass dataset name to step constructor or provide datasetId " +
                "in the flight input params");
        }
        Dataset dataset = datasetDao.retrieve(UUID.fromString(datasetId));
        if (dataset == null) {
            throw new DatasetNotFoundException("Cannot find dataset for " + datasetId);
        }
        return dataset.getName();
    }
}

