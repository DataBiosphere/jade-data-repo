package bio.terra.service.filedata.azure.util;

import static com.azure.core.util.polling.LongRunningOperationStatus.FAILED;
import static com.azure.core.util.polling.LongRunningOperationStatus.IN_PROGRESS;
import static com.azure.core.util.polling.LongRunningOperationStatus.SUCCESSFULLY_COMPLETED;
import static com.azure.core.util.polling.LongRunningOperationStatus.USER_CANCELLED;
import static com.azure.storage.blob.models.CopyStatusType.PENDING;
import static com.azure.storage.blob.models.CopyStatusType.SUCCESS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.CopyStatusType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(Unit.TAG)
class BlobContainerCopyInfoTest {

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
    BlobContainerCopyInfo blobContainerCopyInfo =
        new BlobContainerCopyInfo(createPollResponses(statuses));

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
    return switch (statusType) {
      case SUCCESS -> SUCCESSFULLY_COMPLETED;
      case FAILED -> FAILED;
      case ABORTED -> USER_CANCELLED;
      case PENDING -> IN_PROGRESS;
    };
  }
}
