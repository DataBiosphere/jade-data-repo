package bio.terra.service.filedata.google.firestore;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.DeadlineExceededException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

// Fake future class testing fire store batchOperation.
// This is not thread safe!
public class FakeApiFuture implements ApiFuture<String> {

  private static int testThrowCount;
  private static int currentCount;

  public static void initialize(int throwCount) {
    testThrowCount = throwCount;
    currentCount = 0;
  }

  public static boolean shouldThrow() {
    currentCount++;
    return (currentCount <= testThrowCount);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {}

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return false;
  }

  @Override
  public String get() throws InterruptedException, ExecutionException, DeadlineExceededException {
    throw new RuntimeException("TDR should only use the timeout version of get");
  }

  @Override
  public String get(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (FakeApiFuture.shouldThrow()) {
      throw new TimeoutException("test");
    }
    return "abc";
  }
}
