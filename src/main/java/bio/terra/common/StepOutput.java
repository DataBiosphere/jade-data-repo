package bio.terra.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface StepOutput {
  String USE_DEFAULT_NAME = "";

  String value() default USE_DEFAULT_NAME;
}
