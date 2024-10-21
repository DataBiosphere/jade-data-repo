package bio.terra.common.category;

/**
 * Unit test category. Tests in this category only rely on a postgresql database being configured.
 * That database could be local or CloudSQL accessed through the Cloud SQL Proxy.
 */
public interface Unit {
  String TAG = "bio.terra.common.category.Unit";
}
