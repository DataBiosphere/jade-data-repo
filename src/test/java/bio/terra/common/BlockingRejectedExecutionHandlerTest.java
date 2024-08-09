package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class BlockingRejectedExecutionHandlerTest {
  @Mock private Runnable r;
  @Mock private ThreadPoolExecutor executor;
  @Mock private BlockingQueue<Runnable> queue;
  private final BlockingRejectedExecutionHandler handler = new BlockingRejectedExecutionHandler();

  @Test
  void rejectedExecution() throws InterruptedException {
    when(executor.isShutdown()).thenReturn(false);
    when(executor.getQueue()).thenReturn(queue);

    handler.rejectedExecution(r, executor);
    verify(queue).put(r);
  }

  @Test
  void rejectedExecution_executorShutdown() {
    when(executor.isShutdown()).thenReturn(true);

    RejectedExecutionException exception =
        assertThrows(
            RejectedExecutionException.class, () -> handler.rejectedExecution(r, executor));
    String expectedMessage = BlockingRejectedExecutionHandler.executorShutdownMessage(executor);
    assertThat(exception.getMessage(), equalTo(expectedMessage));
    assertThat(exception.getCause(), nullValue());
  }

  @Test
  void rejectedExecution_InterruptedException() throws InterruptedException {
    when(executor.isShutdown()).thenReturn(false);
    when(executor.getQueue()).thenReturn(queue);
    doThrow(InterruptedException.class).when(queue).put(r);

    RejectedExecutionException exception =
        assertThrows(
            RejectedExecutionException.class, () -> handler.rejectedExecution(r, executor));
    String expectedMessage = BlockingRejectedExecutionHandler.interruptedExceptionMessage(executor);
    assertThat(exception.getMessage(), equalTo(expectedMessage));
    assertThat(exception.getCause(), instanceOf(InterruptedException.class));
    assertTrue(Thread.interrupted());
  }
}
