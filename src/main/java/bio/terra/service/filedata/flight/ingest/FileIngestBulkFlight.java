package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadRequestModel;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationContext;

/*
 * Required input parameters:
 * - DATASET_ID dataset we are loading into
 * - LOAD_TAG is the load tag for this ingest
 * - CONCURRENT_INGESTS is the number of ingests to allow to run in parallel
 * - CONCURRENT_FILES is the number of file loads to run in parallel
 * - REQUEST is a BulkLoadRequestModel
 */

public class FileIngestBulkFlight extends Flight {

    public FileIngestBulkFlight(FlightMap inputParameters,
                                Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        IamService iamService = (IamService)appContext.getBean("iamService");
        LoadService loadService = (LoadService)appContext.getBean("loadService");
        ObjectMapper objectMapper = (ObjectMapper)appContext.getBean("objectMapper");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        String loadTag = inputParameters.get(LoadMapKeys.LOAD_TAG, String.class);
        BulkLoadRequestModel loadRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);
        int maxFailedFileLoads = loadRequest.getMaxFailedFileLoads();
        String profileId = loadRequest.getProfileId();

        int concurrentFiles = inputParameters.get(LoadMapKeys.CONCURRENT_FILES, Integer.class);

        // TODO: for reserving a bulk load slot:
        //    int concurrentIngests = inputParameters.get(LoadMapKeys.CONCURRENT_INGESTS, Integer.class);
        //  We can maybe just use the load tag lock table to know how many are active.

        // The flight plan:
        // 0. Verify authorization to do the ingest
        // 1. Lock the load tag - only one flight operating on a load tag at a time
        // 2. TODO: reserve a bulk load slot to make sure we have the threads to do the flight; abort otherwise (DR-754)
        // 3. Read the file into the load_file table for processing
        // 4. Main loading loop - shared with bulk ingest from a file in a bucket
        // 5. Generate the bulk file response - just the summary information
        // 6. TODO: Copy results into the database BigQuery (DR-694)
        // 7. Clean load_file table
        // 8. TODO: release the bulk load slot (DR-754) - may not need a step if we use the count of locked tags
        // 9. Unlock the load tag
        addStep(new VerifyAuthorizationStep(iamService, IamResourceType.DATASET, datasetId, IamAction.INGEST_DATA));
        addStep(new LoadLockStep(loadService));
        // 2. reserve a bulk load slot
        addStep(new IngestPopulateFileStateFromFileStep(loadService, objectMapper));
        addStep(new IngestDriverStep(loadService, datasetId, loadTag, concurrentFiles, maxFailedFileLoads, profileId));
        addStep(new IngestBulkFileResponseStep(loadService, loadTag));
        // 6. copy results into BigQuery
        addStep(new IngestCleanFileStateStep(loadService));
        // 8. release bulk load slot
        addStep(new LoadUnlockStep(loadService));
    }

}
