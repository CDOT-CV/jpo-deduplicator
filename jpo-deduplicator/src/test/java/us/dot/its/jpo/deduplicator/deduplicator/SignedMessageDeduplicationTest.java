package us.dot.its.jpo.deduplicator.deduplicator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import us.dot.its.jpo.asn.j2735.r2024.MapData.MapDataMessageFrame;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.BsmDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.MapDeduplicatorTopology;
import us.dot.its.jpo.deduplicator.deduplicator.topologies.TimDeduplicatorTopology;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;

/**
 * Signed messages carry IEEE 1609.2 signature metadata ({@code metadata.certMetadata}) whose
 * generation time changes on every message. These tests check that otherwise-identical signed
 * messages are still deduplicated, and that forwarded messages keep their own signature metadata.
 */
class SignedMessageDeduplicationTest {

    private static final String IN = "topic.in";
    private static final String OUT = "topic.out";
    private static final String MAP_REF = "src/test/resources/json/ode_map/sample.ode-map-reference.json";
    private static final String TIM_REF = "src/test/resources/json/ode_tim/sample.ode-tim-reference.json";
    private static final String BSM_REF = "src/test/resources/json/ode_bsm/sample.ode-bsm-reference.json";

    /** Format the ODE emits for certMetadata instants (millisecond precision, UTC). */
    private static final DateTimeFormatter ODE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void identicalSignedMapsAreDeduplicated() throws IOException {
        String reference = read(MAP_REF);
        Instant t = receivedAt(reference);

        List<JsonNode> out = runMap(signed(reference, t, t), signed(reference, t.plusSeconds(1), t.plusSeconds(1)));

        assertEquals(1, out.size(), "identical signed MAP differing only in certMetadata must be dropped");
        assertEquals(t.toString(), certGenerationTime(out.get(0)));
    }

    @Test
    void unsignedThenSignedIdenticalMapIsDeduplicated() throws IOException {
        String reference = read(MAP_REF);
        Instant t = receivedAt(reference);

        List<JsonNode> out = runMap(reference, signed(reference, t.plusSeconds(1), t.plusSeconds(1)));

        assertEquals(1, out.size(), "signature metadata as a whole is excluded from the MAP comparison");
    }

    @Test
    void changedSignedMapIsForwardedWithItsOwnCertMetadata() throws IOException {
        String reference = read(MAP_REF);
        Instant t = receivedAt(reference);

        OdeMessageFrameData changed = DateJsonMapper.getInstance().readValue(reference, OdeMessageFrameData.class);
        MapDataMessageFrame mf = (MapDataMessageFrame) changed.getPayload().getData();
        mf.getValue().getIntersections().get(0).getLaneSet().get(0).getLaneID().setValue(42);

        List<JsonNode> out = runMap(signed(reference, t, t), signed(changed.toJson(), t.plusSeconds(1), t.plusSeconds(1)));

        assertEquals(2, out.size(), "MAP with different content must be forwarded");
        assertEquals(t.toString(), certGenerationTime(out.get(0)));
        assertEquals(t.plusSeconds(1).toString(), certGenerationTime(out.get(1)),
                "forwarded MAP must keep its own certMetadata, not the previous message's");
        assertEquals(t.plusSeconds(1).toString(), out.get(1).path("metadata").path("odeReceivedAt").asText());
    }

    @Test
    void identicalSignedTimsAreDeduplicated() throws IOException {
        String reference = read(TIM_REF);
        Instant t = receivedAt(reference);

        DeduplicatorProperties props = new DeduplicatorProperties();
        props.setKafkaTopicOdeTimJson(IN);
        props.setKafkaTopicDeduplicatedOdeTimJson(OUT);

        List<JsonNode> out = run(new TimDeduplicatorTopology(props).buildTopology(),
                signed(reference, t, t), signed(reference, t.plusSeconds(1), t.plusSeconds(1)));

        assertEquals(1, out.size(), "identical signed TIM differing only in certMetadata must be dropped");
        assertTrue(out.get(0).path("metadata").has("certMetadata"));
    }

    @Test
    void identicalSignedBsmsAreDeduplicated() throws IOException {
        String reference = read(BSM_REF);
        Instant t = receivedAt(reference);

        DeduplicatorProperties props = new DeduplicatorProperties();
        props.setAlwaysIncludeAtSpeed(1);
        props.setenableOdeBsmDeduplication(true);
        props.setMaximumPositionDelta(1);
        props.setMaximumTimeDelta(10000);
        props.setkafkaTopicOdeBsmJson(IN);
        props.setkafkaTopicDeduplicatedOdeBsmJson(OUT);

        List<JsonNode> out = run(new BsmDeduplicatorTopology(props).buildTopology(),
                signed(reference, t, t), signed(reference, t.plusSeconds(1), t.plusSeconds(1)));

        assertEquals(1, out.size(), "identical stationary signed BSM differing only in certMetadata must be dropped");
        assertEquals(t.toString(), certGenerationTime(out.get(0)));
    }

    private List<JsonNode> runMap(String... inputs) {
        DeduplicatorProperties props = new DeduplicatorProperties();
        props.setKafkaTopicOdeMapJson(IN);
        props.setKafkaTopicDeduplicatedOdeMapJson(OUT);
        return run(new MapDeduplicatorTopology(props).buildTopology(), inputs);
    }

    private List<JsonNode> run(Topology topology, String... inputs) {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<Void, String> in = driver.createInputTopic(IN, Serdes.Void().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> out = driver.createOutputTopic(OUT, Serdes.String().deserializer(), Serdes.String().deserializer());
            for (String input : inputs) {
                in.pipeInput(null, input);
            }
            return out.readKeyValuesToList().stream().map((Function<KeyValue<String, String>, JsonNode>) kv -> {
                try {
                    return mapper.readTree(kv.value);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }).toList();
        }
    }

    /** Returns the message with odeReceivedAt set and a certMetadata block generated at {@code generationTime}. */
    private String signed(String json, Instant odeReceivedAt, Instant generationTime) throws IOException {
        ObjectNode root = (ObjectNode) mapper.readTree(json);
        ObjectNode metadata = (ObjectNode) root.get("metadata");
        metadata.put("odeReceivedAt", odeReceivedAt.toString());
        ObjectNode cert = metadata.putObject("certMetadata");
        cert.put("psid", 32);
        cert.put("generationTime", ODE_TIME.format(generationTime));
        cert.put("certificateValidityStart", "2026-11-17T09:00:05.000Z");
        cert.put("certificateValidityEnd", "2026-11-24T10:00:05.000Z");
        return mapper.writeValueAsString(root);
    }

    private String certGenerationTime(JsonNode message) {
        return Instant.parse(message.path("metadata").path("certMetadata").path("generationTime").asText()).toString();
    }

    private Instant receivedAt(String json) throws IOException {
        return Instant.parse(mapper.readTree(json).path("metadata").path("odeReceivedAt").asText());
    }

    private String read(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }
}
