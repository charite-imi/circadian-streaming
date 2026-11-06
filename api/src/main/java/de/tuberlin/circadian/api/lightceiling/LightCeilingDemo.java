package de.tuberlin.circadian.api.lightceiling;

import de.tuberlin.circadian.analytics.lightceiling.DltSchedule;
import de.tuberlin.circadian.analytics.lightceiling.LightSetting;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Light-ceiling demonstrator. Reads each patient's latest circadian metrics from Postgres, turns them into a {@link DltSchedule}, and renders a self-contained
 * HTML+SVG "ceiling" that shifts colour temperature and brightness across the day. If the database has no metrics it falls back to three illustrative example patients, so it always produces a viewable figure.
 *
 * <p>Run (with the api fat jar on the classpath):
 * <pre>{@code
 *   java -cp api/target/circadian-api.jar de.tuberlin.circadian.api.lightceiling.LightCeilingDemo \
 *        --signal HR --out light-ceiling.html
 * }</pre>
 * 
 * implemented with help of AI
 */
public final class LightCeilingDemo {

    private static final int STEPS = 96;   // 15-minute resolution
    private static final int WIDTH = 960;
    private static final int BAR_H = 64;

    private LightCeilingDemo() {
    }

    private record PatientState(String patientId, double acrophaseRad, double strength, String note) {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = parseArgs(args);
        String signal = opt.getOrDefault("signal", "HR");
        Path out = Path.of(opt.getOrDefault("out", "light-ceiling.html"));
        String jdbcUrl = opt.getOrDefault("jdbc-url", "jdbc:postgresql://localhost:5545/circadian");
        String jdbcUser = opt.getOrDefault("jdbc-user", "circadian");
        String jdbcPassword = opt.getOrDefault("jdbc-password", "circadian");

        List<PatientState> patients;
        String source;
        if (opt.containsKey("examples")) {
            patients = examples();
            source = "illustrative example patients (--examples)";
        } else {
            List<PatientState> fromDb = readFromDb(jdbcUrl, jdbcUser, jdbcPassword, signal);
            if (fromDb.isEmpty()) {
                patients = examples();
                source = "illustrative example patients (no metrics found in Postgres)";
            } else {
                patients = fromDb;
                source = "live metrics from Postgres (signal " + signal + ")";
            }
        }

        String html = render(patients, signal, source);
        Files.writeString(out, html);
        System.out.println("Wrote light ceiling for " + patients.size() + " patient(s) to " + out.toAbsolutePath() + " [" + source + "]");
    }

    // --- data ---

    private static List<PatientState> readFromDb(String url, String user, String pass, String signal) {
        Map<String, Double> acro = new LinkedHashMap<>();
        Map<String, Double> r2 = new LinkedHashMap<>();
        Map<String, Double> cri = new LinkedHashMap<>();
        String cosSql = "SELECT DISTINCT ON (patient_id) patient_id, acrophase, r2 "
                + "FROM circadian_metrics WHERE method='cosinor' AND signal_type=? AND acrophase IS NOT NULL "
                + "ORDER BY patient_id, window_end DESC";
        String cwtSql = "SELECT DISTINCT ON (patient_id) patient_id, cri "
                + "FROM circadian_metrics WHERE method='cwt' AND signal_type=? AND cri IS NOT NULL "
                + "ORDER BY patient_id, window_end DESC";
        try (Connection c = DriverManager.getConnection(url, user, pass)) {
            try (PreparedStatement ps = c.prepareStatement(cosSql)) {
                ps.setString(1, signal);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        acro.put(rs.getString(1), rs.getDouble(2));
                        r2.put(rs.getString(1), rs.getDouble(3));
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(cwtSql)) {
                ps.setString(1, signal);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        cri.put(rs.getString(1), rs.getDouble(2));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("No Postgres metrics (" + e.getMessage() + "); using examples.");
            return List.of();
        }

        List<PatientState> out = new ArrayList<>();
        for (var entry : acro.entrySet()) {
            String pid = entry.getKey();
            double strength;
            String note;
            if (cri.containsKey(pid)) {
                strength = cri.get(pid);
                note = "CRI " + fmt(strength, 2);
            } else {
                strength = Math.max(0.0, Math.min(1.0, r2.getOrDefault(pid, 0.5)));
                note = "R² " + fmt(strength, 2) + " (no CRI)";
            }
            out.add(new PatientState(pid, entry.getValue(), strength, note));
        }
        return out;
    }

    private static List<PatientState> examples() {
        return List.of(
                new PatientState("Healthy (peak 15:00)", peak(15.0), 0.90, "strong rhythm"),
                new PatientState("Phase-shifted (peak 21:00)", peak(21.0), 0.70, "shifted, moderate"),
                new PatientState("Disrupted (flat)", peak(15.0), 0.15, "weak rhythm"));
    }

    private static double peak(double hour) {
        return -2.0 * Math.PI * hour / 24.0;
    }

    // --- rendering ---

    private static String render(List<PatientState> patients, String signal, String source) {
        StringBuilder b = new StringBuilder();
        b.append("<!doctype html><html><head><meta charset=\"utf-8\">")
                .append("<title>Circadian Light Ceiling</title><style>")
                .append("body{background:#0b0e14;color:#dfe6f2;font:14px/1.5 system-ui,sans-serif;margin:24px;}")
                .append("h1{font-size:20px;margin:0 0 4px;} .sub{color:#8a94a6;margin-bottom:20px;}")
                .append(".patient{margin:0 0 22px;} .meta{margin-bottom:6px;} .pid{font-weight:600;}")
                .append(".tag{color:#8a94a6;} .axis{color:#6b7688;font-size:11px;}")
                .append("</style></head><body>")
                .append("<h1>Circadian Light Ceiling &mdash; Dynamic Lighting Therapy</h1>")
                .append("<div class=\"sub\">Source: ").append(escape(source))
                .append(". Warm/dim = night, cool/bright = day; the bright block is centred on each "
                        + "patient's rhythm peak, and a weaker rhythm gets a higher-contrast schedule.</div>");

        for (PatientState p : patients) {
            List<LightSetting> sched = DltSchedule.compute(p.acrophaseRad(), p.strength(), STEPS);
            double peakHour = DltSchedule.peakHourFromAcrophase(p.acrophaseRad());
            String strategy = p.strength() < 0.4 ? "aggressive re-entrainment" : "maintenance";
            b.append("<div class=\"patient\"><div class=\"meta\"><span class=\"pid\">")
                    .append(escape(p.patientId())).append("</span> <span class=\"tag\">")
                    .append(" &mdash; peak ").append(hhmm(peakHour)).append(", ")
                    .append(escape(p.note())).append(" &rarr; ").append(strategy)
                    .append("</span></div>");
            b.append(ceilingSvg(sched));
            b.append("</div>");
        }

        b.append(hourAxis());
        b.append("</body></html>");
        return b.toString();
    }

    private static String ceilingSvg(List<LightSetting> sched) {
        double cellW = (double) WIDTH / STEPS;
        StringBuilder svg = new StringBuilder();
        svg.append("<svg width=\"").append(WIDTH).append("\" height=\"").append(BAR_H)
                .append("\" viewBox=\"0 0 ").append(WIDTH).append(' ').append(BAR_H).append("\">");
        // colour-temperature ceiling cells, brightness via opacity
        StringBuilder curve = new StringBuilder();
        for (int i = 0; i < sched.size(); i++) {
            LightSetting ls = sched.get(i);
            double x = i * cellW;
            double opacity = 0.12 + 0.88 * ls.intensity();
            svg.append("<rect x=\"").append(fmt(x, 2)).append("\" y=\"0\" width=\"")
                    .append(fmt(cellW + 0.6, 2)).append("\" height=\"").append(BAR_H)
                    .append("\" fill=\"").append(kelvinToHex(ls.cctKelvin()))
                    .append("\" fill-opacity=\"").append(fmt(opacity, 3)).append("\"/>");
            double cx = x + cellW / 2.0;
            double cy = BAR_H - 3 - ls.intensity() * (BAR_H - 8);
            curve.append(i == 0 ? "M" : "L").append(fmt(cx, 1)).append(' ').append(fmt(cy, 1)).append(' ');
        }
        // intensity curve
        svg.append("<path d=\"").append(curve).append("\" fill=\"none\" stroke=\"#ffffff\" ").append("stroke-opacity=\"0.55\" stroke-width=\"1.5\"/>");
        svg.append("</svg>");
        return svg.toString();
    }

    private static String hourAxis() {
        double cellW = (double) WIDTH / STEPS;
        StringBuilder b = new StringBuilder("<svg class=\"axis\" width=\"").append(WIDTH)
                .append("\" height=\"18\" viewBox=\"0 0 ").append(WIDTH).append(" 18\">");
        for (int h = 0; h <= 24; h += 3) {
            double x = (h / 24.0) * STEPS * cellW;
            if (h == 24) {
                x -= 14;
            }
            b.append("<text x=\"").append(fmt(x + 1, 1)).append("\" y=\"13\" fill=\"#6b7688\">")
                    .append(h).append(":00</text>");
        }
        b.append("</svg>");
        return b.toString();
    }

    /** Correlated-colour-temperature (K) to an approximate sRGB hex (Tanner Helland approximation). */
    static String kelvinToHex(double kelvin) {
        double t = Math.max(1000.0, Math.min(40000.0, kelvin)) / 100.0;
        double r;
        double g;
        double bl;
        if (t <= 66) {
            r = 255;
            g = 99.4708025861 * Math.log(t) - 161.1195681661;
        } else {
            r = 329.698727446 * Math.pow(t - 60, -0.1332047592);
            g = 288.1221695283 * Math.pow(t - 60, -0.0755148492);
        }
        if (t >= 66) {
            bl = 255;
        } else if (t <= 19) {
            bl = 0;
        } else {
            bl = 138.5177312231 * Math.log(t - 10) - 305.0447927307;
        }
        return "#" + hex(r) + hex(g) + hex(bl);
    }

    private static String hex(double channel) {
        int v = (int) Math.round(Math.max(0.0, Math.min(255.0, channel)));
        return String.format(Locale.ROOT, "%02x", v);
    }

    private static String fmt(double v, int decimals) {
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }

    private static String hhmm(double hour) {
        int hh = ((int) Math.floor(hour)) % 24;
        int mm = (int) Math.round((hour - Math.floor(hour)) * 60.0);
        if (mm == 60) {
            mm = 0;
            hh = (hh + 1) % 24;
        }
        return String.format(Locale.ROOT, "%02d:%02d", hh, mm);
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opt = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (key.equals("examples")) {
                    opt.put(key, "true");
                } else if (i + 1 < args.length) {
                    opt.put(key, args[++i]);
                }
            }
        }
        return opt;
    }
}