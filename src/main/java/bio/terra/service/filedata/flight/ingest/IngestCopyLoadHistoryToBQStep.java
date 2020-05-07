package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;


public class IngestCopyLoadHistoryToBQStep implements Step {
    private final Logger logger = LoggerFactory.getLogger(IngestBulkFileResponseStep.class);

    private final LoadService loadService;
    private final String loadTag;

    public IngestCopyLoadHistoryToBQStep(LoadService loadService, String loadTag) {
        this.loadService = loadService;
        this.loadTag = loadTag;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
        UUID loadId = UUID.fromString(loadIdString);

        // for load tag, get files in chunk out of postgres table
        // Want this info:
        // load_tag - have this here!
        // load_time - this will just be on the BQ side of things
        // source_name
        // target_path
        // file_id
        // checksum_crc32c
        // checksum_md5
        // LOOP - get chunks until done
        // call loadDao (via LoadService) w/ load tag and chunk number
        int chunkSize = 1000;
        int chunkNum = 0;
        List<BulkLoadHistoryModel> loadHistoryArray = null;

        while (loadHistoryArray == null || loadHistoryArray.size() == chunkSize) {
            loadHistoryArray = loadService.makeLoadHistoryArray(loadId, chunkSize, chunkNum);
            chunkNum++;
            // send list plus load_tag to BQ to be put in a temporary table
            // Question: How do we identify the dataset that this file is associated with? And therefore which load
            // history table we should add this information to?
        }



        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        /*FlightMap workingMap = context.getWorkingMap();
        String itemId = workingMap.get(FileMapKeys.FILE_ID, String.class);
        try {
            fileDao.deleteFileMetadata(dataset, itemId);
        } catch (FileSystemAbortTransactionException rex) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
        }*/
        return StepResult.getStepResultSuccess();
    }

}
