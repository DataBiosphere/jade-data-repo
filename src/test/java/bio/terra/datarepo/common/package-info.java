package bio.terra.datarepo.common;

/**
 * Categories are the mechanism used by JUnit to group tests. The annotation is documented here:
 *
 * @see <a
 *     https://junit.org/junit4/javadoc/4.12/org/junit/experimental/categories/Categories.html>JUnit4
 *     Categories</a>
 *     <p>In the build.gradle, specific test scenarios have been configured that include/exclude the
 *     categories defined here. You can read about doing that in gradle here:
 * @see <a https://docs.gradle.org/current/userguide/java_testing.html#test_grouping>Gradle Test
 *     Grouping</a>
 *     <p>All Jade tests need to be assigned one or more categories. Each interface within this
 *     package provides one category. Documentation for the semantics of that category is found in
 *     the interface files.
 */
