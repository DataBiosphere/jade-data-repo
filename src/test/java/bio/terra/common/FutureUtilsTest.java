package bio.terra.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import bio.terra.common.category.Unit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class FutureUtilsTest {
  private static Logger logger = LoggerFactory.getLogger(FutureUtilsTest.class);

  private ThreadPoolExecutor executorService;

  @Before
  public void setUp() throws Exception {
    executorService =
        new ThreadPoolExecutor(3, 3, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10));
  }

  @After
  public void afterClass() throws Exception {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testWaitForTasks() {
    final AtomicInteger counter = new AtomicInteger(0);
    final Callable<Integer> action =
        () -> {
          try {
            TimeUnit.SECONDS.sleep(1);
            return counter.getAndIncrement();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    final List<Future<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executorService.submit(action));
    }

    final List<Integer> resolved = FutureUtils.waitFor(futures);
    assertThat(resolved).containsAll(IntStream.range(0, 10).boxed().collect(Collectors.toList()));
    // Note: adding a sleep since getActiveCount represents an approximation of the number of
    // threads active.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(executorService.getActiveCount()).isZero());
  }

  @Test
  public void testWaitForTaskAcrossTwoBatches() throws InterruptedException {
    // Tests two batches of actions being submitted.  The second batch should be shorter (note the
    // sleep).  Ensure
    // that it properly finishes before the first batch and that an exception doesn't cause the
    // first batch to fail

    // Expand the thread queue for this particular test
    if (executorService != null) {
      executorService.shutdown();
    }
    executorService =
        new ThreadPoolExecutor(10, 10, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10));

    final AtomicInteger counter1 = new AtomicInteger(0);
    final Callable<Integer> action1 =
        () -> {
          try {
            TimeUnit.SECONDS.sleep(10);
            return counter1.getAndIncrement();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    final AtomicInteger counter2 = new AtomicInteger(0);
    final Callable<Integer> action2 =
        () -> {
          try {
            if (counter2.get() == 0) {
              throw new RuntimeException("Injected error");
            }
            TimeUnit.MILLISECONDS.sleep(500);
            return counter2.getAndIncrement();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    // Submit the two batches
    final List<Future<Integer>> batch1Futures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      batch1Futures.add(executorService.submit(action1));
    }

    final List<Future<Integer>> batch2Futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      batch2Futures.add(executorService.submit(action2));
    }

    assertThatThrownBy(() -> FutureUtils.waitFor(batch2Futures));

    // There should still be at least 3 services running (from the first batch)
    assertThat(executorService.getActiveCount()).isGreaterThanOrEqualTo(3);
    // Make sure that all tasks from the first batch are still running
    assertThat(CollectionUtils.collect(batch1Futures, Future::isDone)).doesNotContain(true);

    final List<Integer> resolved1 = FutureUtils.waitFor(batch1Futures);
    assertThat(resolved1).containsAll(IntStream.range(0, 3).boxed().collect(Collectors.toList()));
    assertThat(executorService.getActiveCount()).isZero();
  }

  @Test
  public void testWaitForTasksSomeFail() throws InterruptedException {
    final AtomicInteger counter = new AtomicInteger(0);
    final Callable<Integer> action =
        () -> {
          try {
            final int count = counter.getAndIncrement();
            if (count == 5) {
              throw new RuntimeException("Injected error");
            }
            TimeUnit.SECONDS.sleep(1);
            return count;
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    final List<Future<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executorService.submit(action));
    }

    assertThatThrownBy(() -> FutureUtils.waitFor(futures)).hasMessage("Error executing thread");
    // Note: adding a sleep since getActiveCount represents an approximation of the number of
    // threads active.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(executorService.getActiveCount()).isZero());

    // Make sure that some tasks after the failure were cancelled
    assertThat(IterableUtils.countMatches(futures, Future::isCancelled)).isPositive();
  }

  @Test
  public void testThreadTimeoutFailure() throws InterruptedException {
    final AtomicInteger counter = new AtomicInteger(0);
    // Note: Logging statements left in to make understanding test runs easier
    final Callable<Integer> action =
        () -> {
          try {
            // On the first iteration, to cause a failure
            final int count = counter.getAndIncrement();
            logger.info("Launching {}", count);
            if (count == 0) {
              TimeUnit.SECONDS.sleep(1);
            } else {
              // This should allow all other threads to complete successfully
              TimeUnit.MILLISECONDS.sleep(10);
            }
            logger.info("Done with {}", count);
            return count;
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    final List<Future<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      futures.add(executorService.submit(action));
    }

    assertThatThrownBy(() -> FutureUtils.waitFor(futures, Optional.of(Duration.ofMillis(100))))
        .hasMessage("Thread timed out");
    // Note: adding a sleep since getActiveCount represents an approximation of the number of
    // threads active.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(executorService.getActiveCount()).isZero());
  }

  @Test
  public void testMaxOutQueue() {
    final AtomicInteger counter = new AtomicInteger(0);
    final Callable<Integer> action =
        () -> {
          try {
            TimeUnit.MILLISECONDS.sleep(100);
            return counter.getAndIncrement();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    final List<Future<Integer>> futures = new ArrayList<>();
    // Note: we can submit up to 3 running + 10 queue tasks
    for (int i = 0; i < 13; i++) {
      futures.add(executorService.submit(action));
    }

    // Can't accept new threads for now
    assertThatThrownBy(() -> futures.add(executorService.submit(action)));

    final List<Integer> resolved = FutureUtils.waitFor(futures);
    assertThat(resolved).containsAll(IntStream.range(0, 13).boxed().collect(Collectors.toList()));

    futures.clear();
    // Now that queue is empty, we can
    for (int i = 0; i < 10; i++) {
      futures.add(executorService.submit(action));
    }

    final List<Integer> resolvedSecondRound = FutureUtils.waitFor(futures);
    assertThat(resolvedSecondRound)
        .containsAll(IntStream.range(13, 23).boxed().collect(Collectors.toList()));

    // Note: adding a sleep since getActiveCount represents an approximation of the number of
    // threads active.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(executorService.getActiveCount()).isZero());
  }
}
