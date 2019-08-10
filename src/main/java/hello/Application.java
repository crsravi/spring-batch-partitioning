package hello;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Application {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx= SpringApplication.run(Application.class, args);

        ExitCodeGenerator ecg = ctx.getBean(ExitCodeGenerator.class);

        int code = ecg.getExitCode();
        ctx.close();
        System.out.println(code);

    }
}
