package pers.clare.eventjob;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

@SuppressWarnings("unused")
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({EventJobProperties.class})
@Configuration
public @interface EnableEventJob {
    @AliasFor(
            annotation = Configuration.class
    )
    String value() default "";
}
