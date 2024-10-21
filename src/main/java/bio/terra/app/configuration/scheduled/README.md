# Scheduled Background Tasks

One way that we can schedule background tasks within our backend is via **Spring @Scheduled**
annotation with **ShedLock** distributed locking.

By default, Spring cannot handle scheduler synchronization over multiple instances
of an application.  If we deploy our backend with multiple replicas, Spring will try
to run @Scheduled tasks on each replica, which in most cases is not what we want.

A concrete example:
- We currently deploy `jade-datarepo-api` to production with 3 replicas
- Let's say that we have a group membership sync that we want to run hourly
- But we just want this task to run **once** hourly, not hourly on each of our 3 replicas
  - Could result in unexpected behavior, concurrent DB modification issues, etc.

ShedLock is a distributed locking mechanism for Spring @Scheduled tasks.  It ensures
that your @Scheduled tasks are executed at most once at the same time.  If a task
is being executed on a replica, it acquires a lock which prevents execution from another
replica or thread.  In the presence of an active lock, any other attempts at execution
are skipped.

## Spring @Scheduled

### Thread Pool Size

All of an application's Spring @Scheduled tasks run by default off of a singleton thread pool.
This can make debugging difficult: one long-running task will hold the sole thread and prevent
others from being scheduled.

We can increase the size of the pool via the following application property:

```yaml
spring.task.scheduling.pool.size=10
```

### Recommended Schedule

Recommendation: disable Spring @Scheduled jobs by default and conditionally set their schedule
at the deployment level.

The following example disables our job by default using Spring's cron disable expression: `-`.

```java
@Scheduled(cron = "${jobs.cronSchedule:-}")
public void cleanTempDirectory() {
  // do work here
}
```

If we wanted to enable our job, we would need to provide a valid cron expression for
`jobs.cronSchedule` via environment variable, property file, etc.

## [ShedLock](https://github.com/lukas-krecan/ShedLock)

Let's return to the example above.  Here's how we could annotate the method further to manage its
locking with ShedLock, to avoid it being scheduled on every replica:
```java
@Scheduled(cron = "${jobs.cronSchedule:-}")
@SchedulerLock(
  name = "cleanTempDirectory",
  lockAtLeastFor = "5s",
  lockAtMostFor = "5m")
public void cleanTempDirectory() {
  // do work here
}
```

- `name` - required identifier for our job which ShedLock will use in its lock
- `lockAtLeastFor` - optional minimum lock length, to overcome clock differences across nodes
for short-running tasks.
- `lockAtMostFor` - optional maximum lock length, a fallback in case the locking node dies while
the lock is still active.  This should be set much longer than expected execution time.
If unspecified, the default set in @EnableSchedulerLock will be used.

More information on this annotation can be found in
[ShedLock documentation.](https://github.com/lukas-krecan/ShedLock#annotate-your-scheduled-tasks)

## Benefits

By coupling our scheduling with our application code, we hope to make it more clear what we mean
to run as background tasks, even if the specific schedules are configured per deployment.

Outside of setting cron schedules, this approach should not require any modifications to our
Helmfiles or other infrastructure-as-code.

Scheduled tasks running with application code means that the logs emitted from these tasks are found
alongside the rest of our application logs, which should make things easier to debug.

## Caveats

The other side of running scheduled tasks within the application is that they are not insulated
from issues within our main deployment, or vice-versa.  Consider handing resource-intensive
or mission-critical scheduled tasks outside of the main deployment.

More consideration is needed to the matter of alerting on issues with scheduled jobs.
