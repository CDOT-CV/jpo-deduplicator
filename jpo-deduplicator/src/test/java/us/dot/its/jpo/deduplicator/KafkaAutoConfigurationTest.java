package us.dot.its.jpo.deduplicator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Guards against the Spring Boot 4 dependency regression where Kafka auto-configuration is not on
 * the classpath (plain spring-kafka instead of spring-boot-starter-kafka). Without it no
 * {@link KafkaAdmin} bean exists and {@link KafkaConfiguration} fails to start the application.
 */
class KafkaAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class));

    @Test
    void kafkaAutoConfigurationProvidesKafkaAdmin() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(KafkaAdmin.class));
    }
}
