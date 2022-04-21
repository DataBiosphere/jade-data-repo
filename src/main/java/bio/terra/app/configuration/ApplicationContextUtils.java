package bio.terra.app.configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/*
Used in special case to wire in configuration to a non-spring component.
 */
@Component
public class ApplicationContextUtils implements ApplicationContextAware {

  private static ApplicationContext ctx;

  @Override
  @SuppressFBWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification =
          "We need setAppContext to override method in ApplicationContextAware, but need rest of class to be static. Reason for spotbugs: 'This instance method writes to a static field. This is tricky to get correct if multiple instances are being manipulated, and generally bad practice.' Since we are using this specifically to pull a config value that should stay constant, this shouldn't be a concern.")
  public void setApplicationContext(ApplicationContext appContext) {
    ctx = appContext;
  }

  public static ApplicationContext getApplicationContext() {
    if (ctx == null) {
      throw new RuntimeException("The application context isn't initialized yet.");
    }
    return ctx;
  }
}
