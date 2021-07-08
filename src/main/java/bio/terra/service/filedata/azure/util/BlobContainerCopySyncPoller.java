package bio.terra.service.filedata.azure.util;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.models.BlobCopyInfo;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class BlobContainerCopySyncPoller implements SyncPoller<BlobContainerCopyInfo, Void> {

  private final List<SyncPoller<BlobCopyInfo, Void>> blobCopyPollerList;

  public BlobContainerCopySyncPoller(List<SyncPoller<BlobCopyInfo, Void>> blobCopyPollerList) {
    this.blobCopyPollerList = blobCopyPollerList;
  }

  @Override
  public PollResponse<BlobContainerCopyInfo> poll() {
    BlobContainerCopyInfo pollResult =
        new BlobContainerCopyInfo(
            blobCopyPollerList.stream().map(SyncPoller::poll).collect(Collectors.toList()));

    return new PollResponse<>(pollResult.getCopyStatus(), pollResult);
  }

  @Override
  public PollResponse<BlobContainerCopyInfo> waitForCompletion() {

    BlobContainerCopyInfo waitResult =
        new BlobContainerCopyInfo(
            this.blobCopyPollerList.stream()
                .map(SyncPoller::waitForCompletion)
                .collect(Collectors.toList()));

    return new PollResponse<>(waitResult.getCopyStatus(), waitResult);
  }

  @Override
  public PollResponse<BlobContainerCopyInfo> waitForCompletion(Duration timeout) {
    BlobContainerCopyInfo waitResult =
        new BlobContainerCopyInfo(
            this.blobCopyPollerList.stream()
                .map(p -> p.waitForCompletion(timeout))
                .collect(Collectors.toList()));

    return new PollResponse<>(waitResult.getCopyStatus(), waitResult);
  }

  @Override
  public PollResponse<BlobContainerCopyInfo> waitUntil(LongRunningOperationStatus statusToWaitFor) {
    BlobContainerCopyInfo waitResult =
        new BlobContainerCopyInfo(
            this.blobCopyPollerList.stream()
                .map(p -> p.waitUntil(statusToWaitFor))
                .collect(Collectors.toList()));

    return new PollResponse<>(waitResult.getCopyStatus(), waitResult);
  }

  @Override
  public PollResponse<BlobContainerCopyInfo> waitUntil(
      Duration timeout, LongRunningOperationStatus statusToWaitFor) {
    BlobContainerCopyInfo waitResult =
        new BlobContainerCopyInfo(
            this.blobCopyPollerList.stream()
                .map(p -> p.waitUntil(timeout, statusToWaitFor))
                .collect(Collectors.toList()));

    return new PollResponse<>(waitResult.getCopyStatus(), waitResult);
  }

  @Override
  public Void getFinalResult() {
    this.blobCopyPollerList.forEach(SyncPoller::getFinalResult);
    return null;
  }

  @Override
  public void cancelOperation() {
    this.blobCopyPollerList.forEach(SyncPoller::cancelOperation);
  }
}
