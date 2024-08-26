package bio.terra.app.configuration;

import bio.terra.app.logging.LoggerInterceptor;
import bio.terra.app.usermetrics.UserMetricsInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.util.UrlPathHelper;

@Component
public class WebConfig implements WebMvcConfigurer {
  @Autowired private LoggerInterceptor loggerInterceptor;
  @Autowired private UserMetricsInterceptor metricsInterceptor;

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(loggerInterceptor);
    registry.addInterceptor(metricsInterceptor);
  }

  @Override
  public void configurePathMatch(PathMatchConfigurer configurer) {
    // This override is needed in order to allow encoded slashes in the path of a URL.
    UrlPathHelper urlPathHelper = new UrlPathHelper();
    // By setting this to false, Spring does not decode the path before matching it to a
    // method.  Rather, it does it after matching so that by the time the value reaches the
    // controller function, the value is decoded.
    urlPathHelper.setUrlDecode(false);
    configurer.setUrlPathHelper(urlPathHelper);
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry
        .addResourceHandler("/webjars/swagger-ui-dist/**")
        // See https://broadworkbench.atlassian.net/browse/DCJ-641 for reading the version dynamically
        .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui-dist/5.17.14/");
  }
}
