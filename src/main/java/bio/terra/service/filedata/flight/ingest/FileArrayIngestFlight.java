package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.flight.VerifyAuthorizationStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

/*
 * Required input parameters:
 * - DATASET_ID dataset we are loading into
 * - LOAD_TAG is the load tag for this ingest
 * - REQUEST is a BulkLoadArrayRequestModel
 */

public class FileArrayIngestFlight extends Flight {

    public FileArrayIngestFlight(FlightMap inputParameters,
                                 Object applicationContext,
                                 UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        //ApplicationConfiguration appConfig = (ApplicationConfiguration)appContext.getBean("applicationConfiguration");
        IamService iamService = (IamService)appContext.getBean("iamService");
        LoadService loadService = (LoadService)appContext.getBean("loadService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        String loadTag = inputParameters.get(LoadMapKeys.LOAD_TAG, String.class);

        // The flight plan:
        // 0. Verify authorization to do the ingest
        // 1. Lock the load tag - only one flight operating on a load tag at a time
        // 2. TODO: reserve a bulk load slot to make sure we have the threads to do the flight; abort otherwise (DR-754)
        // 3. Put the array into the load_file table for processing
        // 4. Main loading loop - shared with bulk ingest from a file in a bucket
        // 5. TODO: Copy results into the database BigQuery (DR-694)
        // 6. TODO: release the bulk load slot (DR-754)
        // 7. Unlock the load tag
        addStep(new VerifyAuthorizationStep(iamService, IamResourceType.DATASET, datasetId, IamAction.INGEST_DATA));
        addStep(new LoadLockStep(loadService));
        // 2. reserve a bulk load slot
        addStep(new IngestPopulateFileStateFromArrayStep(loadService));
        addStep(new IngestDriverStep(loadService, datasetId, loadTag));
        // copy results into BigQuery
        // release bulk load slot
        addStep(new LoadUnlockStep(loadService));
    }

}
