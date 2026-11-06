package de.tuberlin.circadian.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.tuberlin.circadian.common.model.SignalType;
import de.tuberlin.circadian.common.model.VitalSample;
import de.tuberlin.circadian.common.serde.VitalSampleCodec;
import de.tuberlin.circadian.generator.model.Disruption;
import de.tuberlin.circadian.generator.model.SignalBaseline;
import de.tuberlin.circadian.generator.scenario.Scenario;
import de.tuberlin.circadian.generator.scenario.ScenarioLoader;
import de.tuberlin.circadian.generator.synth.SignalSynthesizer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

// Synthetic vital-sign generator.
@Command(name = "generator", mixinStandardHelpOptions = true, version = "circadian-generator 0.2.0", description = "Generate synthetic ICU vital signs to Kafka and ground truth to Postgres.")
public final class GeneratorMain implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorMain.class);

    @Option(names = "--scenario", description = "YAML scenario file (e.g. scenarios/amplitude_decay.yaml). Overrides --patients/--sim-hours/--rate-hz; --seed still applies.")
    String scenarioPath;

    @Option(names = "--patients", defaultValue = "3", description = "Patients when no --scenario is given (default: ${DEFAULT-VALUE}).")
    int patients;

    @Option(names = "--seed", defaultValue = "42", description = "Master RNG seed; identical seed => identical stream (default: ${DEFAULT-VALUE}).")
    long seed;

    @Option(names = "--sim-hours", defaultValue = "3.0", description = "Simulated hours when no --scenario is given (default: ${DEFAULT-VALUE}).")
    double simHours;

    @Option(names = "--rate-hz", defaultValue = "1.0", description = "Samples per second per signal when no --scenario (default: ${DEFAULT-VALUE}).")
    double rateHz;

    @Option(names = "--start", defaultValue = "2026-06-01T00:00:00Z", description = "Event-time start instant, UTC ISO-8601. Fixed by default for reproducibility.")
    String startIso;

    @Option(names = "--speed", defaultValue = "0", description = "Replay speed: 0 = as fast as possible; N = N simulated seconds per wall second.")
    double speed;

    @Option(names = "--bootstrap", defaultValue = "localhost:9092", description = "Kafka bootstrap servers (default: ${DEFAULT-VALUE}).")
    String bootstrap;

    @Option(names = "--topic", defaultValue = "vitals-raw", description = "Kafka topic (default: ${DEFAULT-VALUE}).")
    String topic;

    @Option(names = "--jdbc-url", defaultValue = "jdbc:postgresql://localhost:5545/circadian", description = "Postgres JDBC URL for ground truth (default: ${DEFAULT-VALUE}).")
    String jdbcUrl;

    @Option(names = "--jdbc-user", defaultValue = "circadian", description = "Postgres user.")
    String jdbcUser;

    @Option(names = "--jdbc-password", defaultValue = "circadian", description = "Postgres password.")
    String jdbcPassword;

    @Option(names = "--skip-ground-truth", defaultValue = "false", description = "Stream to Kafka without writing ground truth to Postgres.")
    boolean skipGroundTruth;

    // One independent synthetic series: a patient/signal with its baseline, disruptions, synthesizer.
    private record Stream(String patientId, SignalType signal, SignalBaseline baseline, List<Disruption> disruptions, SignalSynthesizer synth) {
    }

    @Override
    public Integer call() throws Exception {
        Scenario scenario = (scenarioPath != null) ? ScenarioLoader.load(scenarioPath) : Scenario.cleanDefault(patients, simHours, rateHz);

        String runId = UUID.randomUUID().toString();
        Instant start = Instant.parse(startIso);
        List<Stream> streams = buildStreams(scenario);
        long totalSteps = Math.round(scenario.simHours * 3600.0 * scenario.rateHz);

        LOG.info("Run id {} — scenario '{}': patients={} signals={} streams={} seed={} simHours={} rateHz={} disruptionSpecs={} start={}", runId, scenario.name, scenario.patients, SignalType.values().length, streams.size(), seed, scenario.simHours, scenario.rateHz, scenario.disruptions.size(), start);

        if (!skipGroundTruth) {
            writeGroundTruth(streams, start, runId, scenario.name);
        } else {
            LOG.warn("Skipping ground-truth write (--skip-ground-truth).");
        }

        long produced = streamToKafka(streams, scenario, start, totalSteps);
        LOG.info("Done. Produced {} samples to topic '{}'.", produced, topic);
        return 0;
    }

    private List<Stream> buildStreams(Scenario scenario) {
        List<Stream> streams = new ArrayList<>();
        for (int p = 0; p < scenario.patients; p++) {
            String patientId = String.format("P%04d", p + 1);
            double scale = patientAmplitudeScale(scenario, p);
            for (SignalType signal : SignalType.values()) {
                Random baseRng = new Random(mix(seed, p, signal.ordinal(), 0xB1));
                SignalBaseline baseline = SignalBaseline.randomizedFor(signal, baseRng)
                        .withAmplitudeScaled(scale);
                List<Disruption> disruptions = scenario.disruptionsFor(patientId, signal);
                Random synthRng = new Random(mix(seed, p, signal.ordinal(), 0x57));
                SignalSynthesizer synth = new SignalSynthesizer(
                        baseline, disruptions, scenario.fidelity, synthRng);
                streams.add(new Stream(patientId, signal, baseline, disruptions, synth));
            }
        }
        return streams;
    }

    // Per-patient amplitude scaling drawn from [min,max] (gives a cohort variable rhythm strength).
    private double patientAmplitudeScale(Scenario scenario, int patientIdx) {
        double lo = scenario.fidelity.amplitudeScaleMin;
        double hi = scenario.fidelity.amplitudeScaleMax;
        if (lo == hi) {
            return lo;
        }
        Random rng = new Random(mix(seed, patientIdx, 0, 0xCA));
        return lo + (hi - lo) * rng.nextDouble();
    }

    private void writeGroundTruth(List<Stream> streams, Instant start, String runId, String scenarioName) throws Exception {
        try (GroundTruthWriter writer = new GroundTruthWriter(jdbcUrl, jdbcUser, jdbcPassword)) {
            writer.writeRun(runId, scenarioName, seed, "generator run for scenario '" + scenarioName + "'");
            for (Stream s : streams) {
                writer.writeParams(s.patientId(), s.baseline());
            }
            int events = 0;
            for (Stream s : streams) {
                for (Disruption d : s.disruptions()) {
                    Instant from = start.plusMillis(Math.round(d.startHour() * 3_600_000.0));
                    Instant to = start.plusMillis(Math.round(d.endHour() * 3_600_000.0));
                    writer.writeEvent(s.patientId(), s.signal(), d.type(), from, to, d.magnitude(),
                            paramsJson(d));
                    events++;
                }
            }
            writer.commit();
            LOG.info("Wrote ground truth: {} patient/signal params, {} disruption events.",
                    streams.size(), events);
        } catch (java.sql.SQLException e) {
            LOG.error("Failed to write ground truth to {} (is `docker compose up` running?). Use --skip-ground-truth to stream without Postgres.", jdbcUrl, e);
            throw e;
        }
    }

    private long streamToKafka(List<Stream> streams, Scenario scenario, Instant start, long totalSteps) {
        double dtSeconds = 1.0 / scenario.rateHz;
        long progressEvery = Math.max(1, Math.round(3600.0 * scenario.rateHz));
        long produced = 0;
        long wallStartNanos = System.nanoTime();

        try (Producer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (long step = 0; step < totalSteps; step++) {
                double simSeconds = step * dtSeconds;
                Instant eventTime = start.plusMillis(Math.round(simSeconds * 1000.0));
                double hoursSinceEpoch = eventTime.toEpochMilli() / 3_600_000.0;
                double simHour = simSeconds / 3600.0;

                for (Stream s : streams) {
                    OptionalDouble value = s.synth().next(hoursSinceEpoch, simHour);
                    if (value.isEmpty()) {
                        continue; // gap (missing sample)
                    }
                    VitalSample sample = VitalSample.of(
                            s.patientId(), s.signal(), eventTime, value.getAsDouble());
                    producer.send(new ProducerRecord<>(
                            topic, s.patientId(), VitalSampleCodec.toJson(sample)));
                    produced++;
                }

                throttle(simSeconds, wallStartNanos);
                if (step > 0 && step % progressEvery == 0) {
                    LOG.info("  simulated {} h ({} samples produced)",
                            Math.round(simSeconds / 3600.0), produced);
                }
            }
            producer.flush();
        }
        return produced;
    }

    private static String paramsJson(Disruption d) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("rampHours", d.rampHours());
        params.put("boutHours", d.boutHours());
        try {
            return VitalSampleCodec.mapper().writeValueAsString(params);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize disruption params", e);
        }
    }

    // Pace production to wall-clock when --speed > 0; no-op when 0 (as fast as possible).
    private void throttle(double simSeconds, long wallStartNanos) {
        if (speed <= 0) {
            return;
        }
        long targetNanos = wallStartNanos + Math.round(simSeconds / speed * 1_000_000_000.0);
        long sleepNanos = targetNanos - System.nanoTime();
        if (sleepNanos > 0) {
            try {
                Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(64 * 1024));
        return props;
    }

    // Deterministically derive a stream seed from (master seed, patient, signal, salt). 
    private static long mix(long seed, int patientIdx, int signalOrdinal, int salt) {
        return seed * 1_000_003L + patientIdx * 9176L + signalOrdinal * 131L + salt;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new GeneratorMain()).execute(args);
        System.exit(exitCode);
    }
}