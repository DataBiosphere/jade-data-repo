package bio.terra;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcRegistrations;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.ServletRequestDataBinder;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.annotation.InitBinderDataBinderFactory;
import org.springframework.web.method.support.InvocableHandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.ServletRequestDataBinderFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class Main implements CommandLineRunner {

  @Override
  public void run(String... arg0) throws Exception {
    if (arg0.length > 0 && arg0[0].equals("exitcode")) {
      throw new ExitException();
    }
  }

  public static void main(String[] args) throws Exception {
    SpringApplication theApp = new SpringApplication(Main.class);
    // Initially, Jade runs only with Google cloud parts right now, so we set the profile here.
    // ITFOT, we can parameterize the profile to include the appropriate pdao implementations.
    theApp.setAdditionalProfiles("google");
    theApp.run(args);
  }

  static class ExitException extends RuntimeException implements ExitCodeGenerator {
    private static final long serialVersionUID = 1L;

    @Override
    public int getExitCode() {
      return 10;
    }
  }

  // Mitigation for Spring Core Bug: https://spring.io/blog/2022/03/31/spring-framework-rce-early-announcement
  @Bean
  public WebMvcRegistrations mvcRegistrations() {
    return new WebMvcRegistrations() {
      @Override
      public RequestMappingHandlerAdapter getRequestMappingHandlerAdapter() {
        return new ExtendedRequestMappingHandlerAdapter();
      }
    };
  }


  private static class ExtendedRequestMappingHandlerAdapter extends RequestMappingHandlerAdapter {

    @Override
    protected InitBinderDataBinderFactory createDataBinderFactory(List<InvocableHandlerMethod> methods) {

      return new ServletRequestDataBinderFactory(methods, getWebBindingInitializer()) {

        @Override
        protected ServletRequestDataBinder createBinderInstance(
            @Nullable Object target, String name, NativeWebRequest request) throws Exception {

          ServletRequestDataBinder binder = super.createBinderInstance(target, name, request);
          String[] fields = binder.getDisallowedFields();
          List<String> fieldList = new ArrayList<>(fields != null ? Arrays.asList(fields) : Collections.emptyList());
          fieldList.addAll(Arrays.asList("class.*", "Class.*", "*.class.*", "*.Class.*"));
          binder.setDisallowedFields(fieldList.toArray(new String[] {}));
          return binder;
        }
      };
    }
  }
}
