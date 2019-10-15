package bio.terra.common;

/**
 * StairwayUnit unit test category for the stairway unit tests. We may not want to run these
 * all of the time, since the rate of change is likely slower than the main code.
 * Tests in this category only rely on a postgresql database being configured.
 * That database could be local or CloudSQL accessed through the Cloud SQL Proxy.
 */
public interface StairwayUnit {
}
