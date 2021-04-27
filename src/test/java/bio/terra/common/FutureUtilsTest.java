package bio.terra.common;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category(Unit.class)
public class FutureUtilsTest {

    private static class FutureImpl<V> implements Future<V> {

        boolean isCancelled = false;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return isCancelled = true;
        }

        @Override
        public boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }

    @Test
    public void testWaitForTasks() {
        final AtomicInteger counter = new AtomicInteger(0);
        final Future<Integer> action = new FutureImpl<>() {
            @Override
            public Integer get() {
                return counter.getAndIncrement();
            }
        };
        final List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(action);
        }

        final List<Integer> resolved = FutureUtils.waitFor(futures);
        assertThat(resolved, containsInAnyOrder(IntStream.range(0, 10).boxed().toArray()));
    }

    @Test
    public void testWaitForTaskAcrossTwoBatches() {
        // Tests two batches of actions being submitted.  The second batch should be shorter (note the sleep).  Ensure
        // that it properly finishes before the first batch and that an exception doesn't cause the first batch to fail

        final AtomicInteger counter1 = new AtomicInteger(0);
        final Future<Integer> action1 = new FutureImpl<>() {
            @Override
            public Integer get() {
                return counter1.getAndIncrement();
            }
        };

        final AtomicInteger counter2 = new AtomicInteger(0);
        final Future<Integer> action2 = new FutureImpl<Integer>() {
            @Override
            public Integer get() {
                if (counter2.get() == 0) {
                    throw new RuntimeException("Injected error");
                }
                return counter2.getAndIncrement();
            }
        };

        // Submit the two batches
        final List<Future<Integer>> batch1Futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            batch1Futures.add(action1);
        }

        final List<Future<Integer>> batch2Futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch2Futures.add(action2);
        }

        assertThatThrownBy(() -> FutureUtils.waitFor(batch2Futures));

        // Make sure that all tasks from the first batch are still running
        assertTrue(batch1Futures.stream().noneMatch(Future::isDone));

        final List<Integer> resolved1 = FutureUtils.waitFor(batch1Futures);
        assertThat(resolved1, containsInAnyOrder(IntStream.range(0, 3).boxed().toArray()));
    }

    @Test
    public void testWaitForTasksSomeFail() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        final Future<Integer> action = new FutureImpl<>() {
            @Override
            public Integer get() throws ExecutionException {
                final int count = counter.getAndIncrement();
                if (count == 5) {
                    throw new ExecutionException("Injected error", null);
                }
                return count;
            }
        };

        final List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(action);
        }

        assertThatThrownBy(() -> FutureUtils.waitFor(futures)).hasMessage("Error executing thread");

        // Make sure that some tasks after the failure were cancelled
        assertTrue(futures.stream().anyMatch(Future::isCancelled));
    }
}
