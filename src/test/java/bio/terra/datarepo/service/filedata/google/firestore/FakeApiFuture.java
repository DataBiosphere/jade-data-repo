package bio.terra.datarepo.service.filedata.google.firestore;

import static io.grpc.Status.Code.DEADLINE_EXCEEDED;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.DeadlineExceededException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    if (FakeApiFuture.shouldThrow()) {
      throw new DeadlineExceededException(
          "test",
          new IllegalArgumentException("test"),
          GrpcStatusCode.of(DEADLINE_EXCEEDED),
          false);
    }
    return "abc";
  }

  @Override
  public String get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }
}
