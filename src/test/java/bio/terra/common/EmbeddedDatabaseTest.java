package bio.terra.common;

import io.zonky.test.db.AutoConfigureEmbeddedDatabase;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@AutoConfigureEmbeddedDatabase(
    type = AutoConfigureEmbeddedDatabase.DatabaseType.POSTGRES,
    provider = AutoConfigureEmbeddedDatabase.DatabaseProvider.DOCKER,
    beanName = "embeddedDataSource")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public @interface EmbeddedDatabaseTest {}
