package bio.terra.service.filedata.azure.util;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.storage.blob.models.BlobCopyInfo;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class encapsulates all poll responses for a given copy operation. From the responses the
 * status of the operation is determined.
 */
public class BlobContainerCopyInfo {
  private final List<PollResponse<BlobCopyInfo>> pollResponses;

  public BlobContainerCopyInfo(List<PollResponse<BlobCopyInfo>> pollResponses) {
    this.pollResponses = pollResponses;
  }

  /**
   * Returns the status of the copy operation from the individual poll responses. The implementation
   * follows the these rules in order: SUCCESSFULLY_COMPLETED: When all operations are completed
   * successful. FAILED: When at least one operation has FAILED. USER_CANCELLED: When at least one
   * operation has been cancelled. IN_PROGRESS: When at least one operation is in progress.
   * NOT_STARTED: When at least one operation has not started.
   *
   * @return The status of the long running operation.
   */
  public LongRunningOperationStatus getCopyStatus() {

    var distinctStatuses =
        pollResponses.stream().map(PollResponse::getStatus).collect(Collectors.toSet());

    if (distinctStatuses.size() == 1
        && distinctStatuses.contains(LongRunningOperationStatus.SUCCESSFULLY_COMPLETED)) {
      return LongRunningOperationStatus.SUCCESSFULLY_COMPLETED;
    }

    for (var status :
        List.of(
            LongRunningOperationStatus.FAILED,
            LongRunningOperationStatus.USER_CANCELLED,
            LongRunningOperationStatus.IN_PROGRESS,
            LongRunningOperationStatus.NOT_STARTED)) {
      if (distinctStatuses.contains(status)) {
        return status;
      }
    }

    throw new RuntimeException("Invalid operation status returned");
  }
}
