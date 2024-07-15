package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.stairway.Step;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(Unit.TAG)
class StairwayExceptionFieldsFactoryTest {

  static Stream<Arguments> fromException() {
    var stepClass = Step.class;
    StackTraceElement[] stackTrace = {
      new StackTraceElement("jdk.internal.reflect.GeneratedMethodAccessor980", "method", "file", 0),
      new StackTraceElement(stepClass.getName(), "method", "file", 0)
    };

    var exceptionWithEmptyMessage = new LoadLockedException("");
    exceptionWithEmptyMessage.setStackTrace(stackTrace);
    var fallbackMessage =
        StairwayExceptionFieldsFactory.getFallbackStepExceptionMessage(
            stepClass.getSimpleName(), exceptionWithEmptyMessage);

    var originalExceptionMessage = "Message from original exception";
    var exceptionWithMessage = new LoadLockedException(originalExceptionMessage);
    exceptionWithMessage.setStackTrace(stackTrace);

    return Stream.of(
        // 1. An exception with an empty message will craft a replacement message
        Arguments.of(exceptionWithEmptyMessage, fallbackMessage),
        // 2. An exception with a message will pass along the message
        Arguments.of(exceptionWithMessage, originalExceptionMessage));
  }

  @ParameterizedTest
  @MethodSource
  void fromException(Exception originalException, String expectedMessage) {
    var actual = StairwayExceptionFieldsFactory.fromException(originalException);
    assertThat(actual.getMessage(), equalTo(expectedMessage));
  }
}
