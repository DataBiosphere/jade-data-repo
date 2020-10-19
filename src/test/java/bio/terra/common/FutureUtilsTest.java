package bio.terra.common;

import bio.terra.common.category.Unit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class FutureUtilsTest {
    private static Logger logger = LoggerFactory.getLogger(FutureUtilsTest.class);

    private ThreadPoolExecutor executorService;

    @Before
    public void setUp() throws Exception {
        executorService = new ThreadPoolExecutor(
            3,
            3,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(10)
        );
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
        final Callable<Integer> action = () -> {
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
        assertThat(executorService.getActiveCount()).isZero();
    }

    @Test
    public void testWaitForTaskAcrossTwoBatches() {
        // Tests two batches of actions being submitted.  The second batch should be shorter (note the sleep).  Ensure
        // that it properly finishes before the first batch and that an exception doesn't cause the first batch to fail

        // Expand the thread queue for this particular test
        if (executorService != null) {
            executorService.shutdown();
        }
        executorService = new ThreadPoolExecutor(
            10,
            10,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(10)
        );

        final AtomicInteger counter1 = new AtomicInteger(0);
        final Callable<Integer> action1 = () -> {
            try {
                TimeUnit.SECONDS.sleep(10);
                return counter1.getAndIncrement();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        final AtomicInteger counter2 = new AtomicInteger(0);
        final Callable<Integer> action2 = () -> {
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
        // There should still be 3 services running
        assertThat(executorService.getActiveCount()).isEqualTo(3);

        final List<Integer> resolved1 = FutureUtils.waitFor(batch1Futures);
        assertThat(resolved1).containsAll(IntStream.range(0, 3).boxed().collect(Collectors.toList()));
        assertThat(executorService.getActiveCount()).isZero();
    }

    @Test
    public void testWaitForTasksSomeFail() {
        final AtomicInteger counter = new AtomicInteger(0);
        final Callable<Integer> action = () -> {
            try {
                if (counter.get() > 5) {
                    throw new RuntimeException("Injected error");
                }
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

        assertThatThrownBy(() -> FutureUtils.waitFor(futures)).hasMessage("Error executing thread");
        assertThat(executorService.getActiveCount()).isZero();
    }

    @Test
    public void testThreadTimeoutFailure() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        // Note: Logging statements left in to make understanding test runs easier
        final Callable<Integer> action = () -> {
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
        // Note: adding a sleep since getActiveCount represents an approximation of the number of threads active.
        // Pausing gives the thread executor a chance to learn that the thread has been canceled
        TimeUnit.MILLISECONDS.sleep(50);
        assertThat(executorService.getActiveCount()).isZero();
    }

    @Test
    public void testMaxOutQueue() {
        final AtomicInteger counter = new AtomicInteger(0);
        final Callable<Integer> action = () -> {
            try {
                TimeUnit.SECONDS.sleep(1);
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
        assertThat(resolvedSecondRound).containsAll(IntStream.range(13, 23).boxed().collect(Collectors.toList()));

        assertThat(executorService.getActiveCount()).isZero();
    }
}
