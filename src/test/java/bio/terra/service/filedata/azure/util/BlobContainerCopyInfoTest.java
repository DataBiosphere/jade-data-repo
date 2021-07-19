package bio.terra.service.filedata.azure.util;

import static com.azure.core.util.polling.LongRunningOperationStatus.*;
import static com.azure.storage.blob.models.CopyStatusType.PENDING;
import static com.azure.storage.blob.models.CopyStatusType.SUCCESS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.CopyStatusType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlobContainerCopyInfoTest {
  private BlobContainerCopyInfo blobContainerCopyInfo;

  @Mock private SyncPoller<BlobCopyInfo, Void> poller;

  private static Stream<Arguments> getCopyStatusScenario() {
    return Stream.of(
        Arguments.of(SUCCESSFULLY_COMPLETED, new CopyStatusType[] {SUCCESS}),
        Arguments.of(SUCCESSFULLY_COMPLETED, new CopyStatusType[] {SUCCESS, SUCCESS}),
        Arguments.of(FAILED, new CopyStatusType[] {SUCCESS, CopyStatusType.FAILED}),
        Arguments.of(
            FAILED, new CopyStatusType[] {SUCCESS, CopyStatusType.FAILED, CopyStatusType.FAILED}),
        Arguments.of(FAILED, new CopyStatusType[] {SUCCESS, PENDING, CopyStatusType.FAILED}),
        Arguments.of(
            USER_CANCELLED, new CopyStatusType[] {SUCCESS, PENDING, CopyStatusType.ABORTED}),
        Arguments.of(IN_PROGRESS, new CopyStatusType[] {SUCCESS, PENDING, PENDING}));
  }

  @ParameterizedTest
  @MethodSource("getCopyStatusScenario")
  void getCopyStatus_ScenarioIsProvided_ExpectedStatusIsReturned(
      LongRunningOperationStatus expectedStatus, CopyStatusType... statuses) {
    blobContainerCopyInfo = new BlobContainerCopyInfo(createPollResponses(statuses));

    assertThat(blobContainerCopyInfo.getCopyStatus(), equalTo(expectedStatus));
  }

  private List<PollResponse<BlobCopyInfo>> createPollResponses(CopyStatusType... statuses) {

    return Arrays.stream(statuses)
        .map(
            s ->
                new PollResponse<>(
                    toLongRunningOperationStatus(s),
                    new BlobCopyInfo("source", "123", s, "etag", null, null)))
        .collect(Collectors.toList());
  }

  private LongRunningOperationStatus toLongRunningOperationStatus(CopyStatusType statusType) {
    switch (statusType) {
      case SUCCESS:
        return SUCCESSFULLY_COMPLETED;
      case FAILED:
        return FAILED;
      case ABORTED:
        return USER_CANCELLED;
      case PENDING:
        return IN_PROGRESS;
    }

    return NOT_STARTED;
  }
}
