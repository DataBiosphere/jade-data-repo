package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.dao.DatasetDao;
import bio.terra.service.dataset.flight.create.DeleteDatasetAssetStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class RemoveAssetSpecFlight extends Flight {

    public RemoveAssetSpecFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");

        // create job to remove the assetspec from the dataset
        addStep(new DeleteDatasetAssetStep(datasetDao));
    }
}
