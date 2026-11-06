"""Evaluation harness for the circadian pipeline (RQ1/RQ2/RQ3).

Two responsibilities, deliberately separated ("experiment ran" vs "figure produced"):

  --snapshot <scenario>   Dump the pipeline output + ground truth currently in Postgres to
                          data/<scenario>/*.csv. Run this right after a scenario's pipeline run,
                          BEFORE the DB is truncated for the next scenario.

  --exp A|B|C|D           Read the CSV snapshots and produce figures/ + a printed results block
                          (numbers that go straight into the thesis). No DB needed.

Experiments (docs/EVALUATION.md):
  A  Metric correctness (RQ1) — pipeline recovers ground-truth M/A/phi from `baseline`.
  B  CWT vs Cosinor (RQ2)     — detection latency vs known onset on the *_long scenarios (headline).
  C  Performance (RQ1)        — throughput/latency vs load (fed a perf CSV; see run_scenario.ps1).
  D  Cohort separation (RQ3)  — metrics separate preserved vs disrupted patients (`cohort`).

All metrics come from the actual Flink pipeline output (`circadian_metrics`); ground truth from the
generator (`ground_truth_params` / `ground_truth_events`). Detection rules are stated explicitly below.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

DSN = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5545")),
    dbname=os.getenv("PGDATABASE", "circadian"),
    user=os.getenv("PGUSER", "circadian"),
    password=os.getenv("PGPASSWORD", "circadian"),
)

TABLES = ["agg_1min", "circadian_metrics", "ground_truth_params", "ground_truth_events"]
TIME_COLS = {"window_start", "window_end", "start_time", "end_time"}

# Pipeline config (must mirror PipelineConfig defaults — used to know when a trailing window is clean).
COSINOR_HOURS = 24
CWT_DAYS = 5
CWT_HOURS = CWT_DAYS * 24
HOP_HOURS = 1

# Detection rule parameters.
K_MAIN = 3.0          # detect at |z| >= K_MAIN ...
N_CONSEC = 3          # ... sustained for N_CONSEC consecutive hourly hops.
K_SWEEP = np.round(np.arange(1.0, 6.01, 0.5), 2)

# Chronobiology colours (consistent across figures).
COL = {"cwt": "#1b9e77", "cosinor": "#7570b3", "isiv": "#d95f02", "onset": "#e11d48", "grid": "#e8eef5"}

plt.rcParams.update({
    "figure.dpi": 130, "savefig.dpi": 150, "font.size": 11,
    "axes.grid": True, "grid.color": COL["grid"], "grid.linewidth": 0.8,
    "axes.edgecolor": "#94a3b8", "axes.linewidth": 0.8, "figure.facecolor": "white",
})


# --------------------------------------------------------------------------- I/O
def _conn():
    import psycopg2
    return psycopg2.connect(**DSN)


def _read_sql(conn, sql):
    import psycopg2.extras
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return pd.DataFrame([dict(r) for r in cur.fetchall()])


def snapshot(scenario):
    outdir = DATA / scenario
    outdir.mkdir(parents=True, exist_ok=True)
    conn = _conn()
    try:
        for t in TABLES:
            df = _read_sql(conn, f"SELECT * FROM {t}")
            df.to_csv(outdir / f"{t}.csv", index=False)
            print(f"  snapshot {scenario}/{t}: {len(df)} rows")
    finally:
        conn.close()


def load(scenario, table):
    fp = DATA / scenario / f"{table}.csv"
    if not fp.exists():
        raise FileNotFoundError(f"missing snapshot: {fp} (run --snapshot {scenario} first)")
    df = pd.read_csv(fp)
    for c in df.columns:
        if c in TIME_COLS:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


# --------------------------------------------------------------------------- helpers
def circ_mean(rad):
    rad = np.asarray(rad, float)
    return np.arctan2(np.nanmean(np.sin(rad)), np.nanmean(np.cos(rad)))


def circ_dist_hours(a, b):
    """Signed circular distance a-b mapped to hours in (-12, 12]."""
    d = np.angle(np.exp(1j * (np.asarray(a, float) - b)))
    return d * 24.0 / (2.0 * np.pi)


def first_sustained(times_h, z, k, n=N_CONSEC):
    """First time (from `times_h`) at which z >= k for n consecutive samples; nan if never."""
    run = 0
    for i in range(len(z)):
        if np.isfinite(z[i]) and z[i] >= k:
            run += 1
            if run >= n:
                return times_h[i - n + 1]
        else:
            run = 0
    return np.nan


# --------------------------------------------------------------------------- metric extraction
def metric_frames(scenario):
    """Return a dict (patient, signal, metric) -> DataFrame[t (UTC), h_since_onset, value],
    plus a dict (patient, signal) -> onset Timestamp (from ground truth). Only patient/signals
    with a disruption event get an onset; clean ones are skipped for detection."""
    m = load(scenario, "circadian_metrics")
    ev = load(scenario, "ground_truth_events")
    onset = {(r.patient_id, r.signal_type): r.start_time for r in ev.itertuples()}

    cos = m[m.method == "cosinor"]
    cwt = m[m.method == "cwt"]
    isiv = m[m.method == "isiv"]

    series = {}
    def add(df, col, name):
        for (p, s), g in df.groupby(["patient_id", "signal_type"]):
            g = g.sort_values("window_end")
            v = g[col].to_numpy(float)
            if np.all(np.isnan(v)):
                continue
            series[(p, s, name)] = pd.DataFrame({"t": g["window_end"].to_numpy(), "value": v})

    add(cos, "amplitude", "cos_amplitude")
    add(cos, "r2", "cos_r2")
    add(cos, "acrophase", "cos_acrophase")
    add(cwt, "cri", "cri")
    add(isiv, "is_stability", "is")
    add(isiv, "iv_variab", "iv")
    return series, onset, ev


# Which metric each method uses to detect each disruption type, and the disruption direction.
#   sign: -1 => a DECREASE signals disruption; +1 => an INCREASE; "circ" => acrophase shift magnitude.
DETECTORS = {
    "amplitude_drop": {
        "Cosinor (amplitude)": ("cos_amplitude", -1),
        "CWT (CRI)":           ("cri", -1),
        "IS/IV (IV)":          ("iv", +1),
    },
    "phase_shift": {
        "Cosinor (acrophase)": ("cos_acrophase", "circ"),
        "CWT (CRI)":           ("cri", -1),
        "IS/IV (IS)":          ("is", -1),
    },
    "fragmentation": {
        "Cosinor (R²)":   ("cos_r2", -1),
        "CWT (CRI)":           ("cri", -1),
        "IS/IV (IV)":          ("iv", +1),
    },
}
MIN_FILL = {"cos_amplitude": COSINOR_HOURS, "cos_r2": COSINOR_HOURS, "cos_acrophase": COSINOR_HOURS,
            "cri": CWT_HOURS, "is": CWT_HOURS, "iv": CWT_HOURS}


def zscore_series(df, onset, metric, sign, scenario_start):
    """Return (t_hours_since_onset, z) where z>=k means 'looks disrupted'. Baseline stats come from
    the clean pre-onset region (window fully before onset AND trailing window filled)."""
    t = df["t"]
    h = (t - onset).dt.total_seconds().to_numpy() / 3600.0
    v = df["value"].to_numpy(float)
    start_h = (t - scenario_start).dt.total_seconds().to_numpy() / 3600.0
    fill = MIN_FILL[metric]
    base_mask = (h <= 0) & (start_h >= fill)      # clean, fully-filled trailing window
    if base_mask.sum() < 6:
        return h, np.full_like(h, np.nan, float)

    if sign == "circ":
        mu = circ_mean(v[base_mask])
        resid_h = circ_dist_hours(v, mu)
        base_sd = np.std(np.abs(circ_dist_hours(v[base_mask], mu))) or 1e-6
        z = np.abs(resid_h) / base_sd
    else:
        mu = np.nanmean(v[base_mask])
        sd = np.nanstd(v[base_mask]) or 1e-6
        z = sign * (v - mu) / sd
    # Blank the warm-up region (trailing window not yet full => CRI/IS-IV on mean-padded data are
    # meaningless). Excluding it from z removes it from the plot, the detection, and the FP check.
    z = np.where(start_h < fill, np.nan, z)
    return h, z


# --------------------------------------------------------------------------- Experiment A
def exp_a(scenario="baseline"):
    print(f"\n=== Experiment A — metric correctness ({scenario}) ===")
    m = load(scenario, "circadian_metrics")
    gt = load(scenario, "ground_truth_params")
    cos = m[m.method == "cosinor"]
    if cos.empty:
        print("  no cosinor metrics in snapshot — did the pipeline run and 24h window fill?")
        return

    rows = []
    for (p, s), g in cos.groupby(["patient_id", "signal_type"]):
        truth = gt[(gt.patient_id == p) & (gt.signal_type == s)]
        if truth.empty:
            continue
        truth = truth.iloc[0]
        amp_e = np.nanmedian(g.amplitude); mes_e = np.nanmedian(g.mesor)
        acr_e = circ_mean(g.acrophase.to_numpy(float))
        ph_err = abs(circ_dist_hours(acr_e, truth.acrophase))
        rows.append(dict(patient=p, signal=s, n=len(g),
                         amp_true=truth.amplitude, amp_est=amp_e,
                         amp_pct=100 * abs(amp_e - truth.amplitude) / max(abs(truth.amplitude), 1e-9),
                         mesor_true=truth.mesor, mesor_est=mes_e,
                         mesor_pct=100 * abs(mes_e - truth.mesor) / max(abs(truth.mesor), 1e-9),
                         phase_err_h=ph_err))
    df = pd.DataFrame(rows)

    print("\n  Per-signal recovery (median over patients):")
    print(f"  {'signal':9} {'amp %err':>9} {'mesor %err':>11} {'phase err (h)':>14}")
    per_sig = df.groupby("signal").median(numeric_only=True)
    for s, r in per_sig.iterrows():
        print(f"  {s:9} {r.amp_pct:9.2f} {r.mesor_pct:11.2f} {r.phase_err_h:14.3f}")
    print(f"  {'OVERALL':9} {df.amp_pct.median():9.2f} {df.mesor_pct.median():11.2f} {df.phase_err_h.median():14.3f}")

    # Figure: estimated vs true amplitude + phase error per signal.
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.4))
    sigs = sorted(df.signal.unique())
    cmap = plt.get_cmap("tab10")
    for i, s in enumerate(sigs):
        d = df[df.signal == s]
        ax1.scatter(d.amp_true, d.amp_est, s=48, color=cmap(i), label=s, zorder=3, edgecolor="white", linewidth=0.6)
    lim = [0, max(df.amp_true.max(), df.amp_est.max()) * 1.1]
    ax1.plot(lim, lim, "--", color="#94a3b8", zorder=1, label="y = x (perfect)")
    ax1.set_xlim(lim); ax1.set_ylim(lim)
    ax1.set_xlabel("true amplitude (ground truth)"); ax1.set_ylabel("recovered amplitude (Cosinor)")
    ax1.set_title("Amplitude recovery"); ax1.legend(fontsize=8, loc="upper left")

    ph = df.groupby("signal").phase_err_h.median().reindex(sigs)
    ax2.bar(range(len(sigs)), ph.values, color=[cmap(i) for i in range(len(sigs))])
    ax2.set_xticks(range(len(sigs))); ax2.set_xticklabels(sigs, rotation=0)
    ax2.set_ylabel("median |phase error| (hours)"); ax2.set_title("Acrophase recovery")
    ax2.set_ylim(0, 0.5)   # fixed scale up to the 30-minute tolerance so the errors are read against it
    fig.suptitle(f"Experiment A — metric correctness on '{scenario}'", fontweight="bold")
    fig.tight_layout()
    out = FIG / "expA_correctness.png"
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    print(f"\n  figure -> {out}")
    df.to_csv(FIG / "expA_recovery.csv", index=False)


# --------------------------------------------------------------------------- Experiment B
def exp_b(scenarios):
    print(f"\n=== Experiment B — CWT vs Cosinor detection latency ({', '.join(scenarios)}) ===")
    all_rows = []       # per (scenario, patient, signal, method) latency at K_MAIN
    sweep_rows = []     # per (scenario, method, k) latency + false-positive flag
    traj = {}           # representative trajectories for the trace figure

    for scenario in scenarios:
        series, onset, ev = metric_frames(scenario)
        agg = load(scenario, "agg_1min")
        scenario_start = agg.window_start.min()
        etype = ev.event_type.mode().iloc[0] if not ev.empty else None
        detset = DETECTORS.get(etype, {})
        if not detset:
            print(f"  {scenario}: no detectors for event_type={etype}; skipping")
            continue

        for (p, s), t_onset in onset.items():
            for label, (metric, sign) in detset.items():
                key = (p, s, metric)
                if key not in series:
                    continue
                h, z = zscore_series(series[key], t_onset, metric, sign, scenario_start)
                post = h > 0
                base = h <= 0
                lat = first_sustained(h[post], z[post], K_MAIN) if post.any() else np.nan
                all_rows.append(dict(scenario=scenario, event=etype, patient=p, signal=s,
                                     method=label, latency_h=lat,
                                     detected=np.isfinite(lat)))
                for k in K_SWEEP:
                    l = first_sustained(h[post], z[post], k) if post.any() else np.nan
                    fp = np.isfinite(first_sustained(h[base], z[base], k)) if base.any() else False
                    sweep_rows.append(dict(scenario=scenario, method=label, k=k,
                                           latency_h=l, detected=np.isfinite(l), false_positive=bool(fp)))
                # keep one representative patient/signal trajectory per method for the trace fig
                traj.setdefault(scenario, {}).setdefault(label, (h, z, t_onset))

    lat = pd.DataFrame(all_rows)
    swp = pd.DataFrame(sweep_rows)
    if lat.empty:
        print("  no detections computed — check snapshots exist for the *_long scenarios.")
        return

    # ---- headline table: median latency + detection rate per scenario/method at K_MAIN ----
    print(f"\n  Detection latency at k={K_MAIN}, sustained {N_CONSEC}h (median hours over patients; "
          f"'—' = not detected):")
    print(f"  {'scenario':22}{'method':22}{'median lat (h)':>15}{'detect rate':>13}")
    for scen in scenarios:
        sub = lat[lat.scenario == scen]
        for method in sub.method.unique():
            d = sub[sub.method == method]
            med = np.nanmedian(d.latency_h) if d.detected.any() else np.nan
            rate = d.detected.mean()
            mstr = f"{med:7.1f}" if np.isfinite(med) else "     — "
            print(f"  {scen:22}{method:22}{mstr:>15}{rate*100:11.0f}%")

    # ---- figure 1: grouped-bar median latency, CWT vs Cosinor vs IS/IV, per scenario ----
    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    methods = ["Cosinor", "CWT", "IS/IV"]
    def bucket(m):
        return "Cosinor" if m.startswith("Cosinor") else "CWT" if m.startswith("CWT") else "IS/IV"
    colors = {"Cosinor": COL["cosinor"], "CWT": COL["cwt"], "IS/IV": COL["isiv"]}
    x = np.arange(len(scenarios)); w = 0.26
    for j, mb in enumerate(methods):
        vals, rates = [], []
        for scen in scenarios:
            d = lat[(lat.scenario == scen) & (lat.method.map(bucket) == mb)]
            v = np.nanmedian(d.latency_h) if (not d.empty and d.detected.any()) else np.nan
            vals.append(v); rates.append(d.detected.mean() if not d.empty else 0)
        bars = ax.bar(x + (j - 1) * w, np.nan_to_num(vals, nan=0.0), w, label=mb, color=colors[mb])
        for xi, (v, r) in enumerate(zip(vals, rates)):
            if not np.isfinite(v):
                ax.text(x[xi] + (j - 1) * w, 1, "n/d", ha="center", va="bottom", fontsize=8, color="#64748b", rotation=90)
    ax.set_xticks(x); ax.set_xticklabels([s.replace("_long", "") for s in scenarios])
    ax.set_ylabel("median detection latency (hours after onset)")
    ax.set_title("Experiment B — detection latency by method (lower = earlier)", fontweight="bold")
    ax.legend(title="method")
    fig.tight_layout(); out = FIG / "expB_latency.png"; fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    print(f"\n  figure -> {out}")

    # ---- figure 2: representative z(t) trajectories near onset (why one detects earlier) ----
    # The z-scores rise near-vertically once a disruption bites, so the informative part is the few
    # hours around onset where each metric crosses k -- zoom there and mark each crossing point.
    n = len(scenarios)
    XLO, XHI, YHI = -12, 24, 14
    fig, axes = plt.subplots(1, n, figsize=(5.2 * n, 4.2), squeeze=False)
    for ci, scen in enumerate(scenarios):
        ax = axes[0][ci]
        for label, (h, z, t_onset) in traj.get(scen, {}).items():
            mb = bucket(label)
            ax.plot(h, z, color=colors[mb], label=label, lw=1.9)
            post = h > 0
            cross = first_sustained(h[post], z[post], K_MAIN) if post.any() else np.nan
            if np.isfinite(cross) and cross <= XHI:  # dot where this metric declares detection
                ax.plot([cross], [K_MAIN], "o", color=colors[mb], ms=8, zorder=6,
                        markeredgecolor="white", markeredgewidth=1.0)
        ax.axhline(K_MAIN, ls="--", color="#64748b", lw=1, label=f"threshold k={K_MAIN}")
        ax.axvline(0, ls="-", color=COL["onset"], lw=1.4, label="onset")
        ax.set_xlabel("hours since onset"); ax.set_title(scen.replace("_long", ""))
        ax.set_xlim(XLO, XHI); ax.set_ylim(-1, YHI)
        if ci == 0:
            ax.set_ylabel("disruption z-score")
        ax.legend(fontsize=7, loc="upper left")
    fig.suptitle("Experiment B — z-score trajectories near onset (representative patient; "
                 "● marks each metric's threshold crossing)", fontweight="bold")
    fig.tight_layout(); out = FIG / "expB_trajectories.png"; fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    print(f"  figure -> {out}")

    # ---- figure 3: latency vs threshold sweep (specificity trade-off) ----
    fig, axes = plt.subplots(1, n, figsize=(5.2 * n, 4.0), squeeze=False)
    for ci, scen in enumerate(scenarios):
        ax = axes[0][ci]
        for mb in methods:
            sub = swp[(swp.scenario == scen) & (swp.method.map(bucket) == mb)]
            med = sub.groupby("k").latency_h.median()
            ax.plot(med.index, med.values, marker="o", ms=3, color=colors[mb], label=mb)
            # mark smallest k with zero false positives (specific operating point)
            fp_by_k = sub.groupby("k").false_positive.mean()
            spec_k = fp_by_k[fp_by_k == 0].index.min() if (fp_by_k == 0).any() else None
            if spec_k is not None and spec_k in med.index and np.isfinite(med.loc[spec_k]):
                ax.scatter([spec_k], [med.loc[spec_k]], s=90, facecolor="none", edgecolor=colors[mb], lw=1.8, zorder=5)
        ax.set_xlabel("threshold k (σ)"); ax.set_title(scen.replace("_long", ""))
        if ci == 0:
            ax.set_ylabel("median latency (h)")
        ax.legend(fontsize=8)
    fig.suptitle("Experiment B — latency vs detection threshold (○ = tightest false-positive-free k)", fontweight="bold")
    fig.tight_layout(); out = FIG / "expB_threshold_sweep.png"; fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    print(f"  figure -> {out}")

    lat.to_csv(FIG / "expB_latency.csv", index=False)
    swp.to_csv(FIG / "expB_sweep.csv", index=False)


# --------------------------------------------------------------------------- Experiment D
def exp_d(scenario="cohort"):
    print(f"\n=== Experiment D — preserved vs disrupted cohort ({scenario}) ===")
    from scipy import stats
    m = load(scenario, "circadian_metrics")
    ev = load(scenario, "ground_truth_events")
    disrupted_p = set(ev.patient_id.unique())
    disrupted_s = sorted(ev.signal_type.unique())     # separation lives on the disrupted signals

    # per patient/signal median of each available metric
    parts = [m[m.method == "cosinor"].groupby(["patient_id", "signal_type"])
             .agg(amplitude=("amplitude", "median"), r2=("r2", "median"))]
    if (m.method == "cwt").any():
        parts.append(m[m.method == "cwt"].groupby(["patient_id", "signal_type"]).agg(cri=("cri", "median")))
    if (m.method == "isiv").any():
        parts.append(m[m.method == "isiv"].groupby(["patient_id", "signal_type"])
                     .agg(iv=("iv_variab", "median"), is_stab=("is_stability", "median")))
    S = pd.concat(parts, axis=1).reset_index()
    S["group"] = np.where(S.patient_id.isin(disrupted_p), "disrupted", "preserved")
    D = S[S.signal_type.isin(disrupted_s)]            # HR / ABP_SYS only
    metrics = [c for c in ["amplitude", "r2", "cri", "iv"] if c in D.columns]

    print(f"  disrupted signals: {disrupted_s} | preserved patients {sorted(set(S.patient_id)-disrupted_p)} "
          f"vs disrupted {sorted(disrupted_p)}")
    print(f"\n  {'metric':9}{'preserved':>11}{'disrupted':>11}{'Cohen d':>10}{'MWU p':>9}")
    for metric in metrics:
        a = D[D.group == "preserved"][metric].dropna()
        b = D[D.group == "disrupted"][metric].dropna()
        if len(a) < 2 or len(b) < 2:
            continue
        pooled = np.sqrt(((len(a)-1)*a.std()**2 + (len(b)-1)*b.std()**2) / (len(a)+len(b)-2)) or 1e-9
        d = (a.mean() - b.mean()) / pooled            # +ve d => preserved > disrupted
        p = stats.mannwhitneyu(a, b, alternative="two-sided").pvalue
        print(f"  {metric:9}{a.mean():11.3f}{b.mean():11.3f}{d:10.2f}{p:9.3f}")

    fig, axes = plt.subplots(1, len(metrics), figsize=(3.4 * len(metrics), 4.2), squeeze=False)
    for ax, metric in zip(axes[0], metrics):
        data = [D[D.group == grp][metric].dropna() for grp in ["preserved", "disrupted"]]
        bp = ax.boxplot(data, tick_labels=["preserved", "disrupted"], patch_artist=True, widths=0.55)
        for patch, c in zip(bp["boxes"], [COL["cwt"], COL["isiv"]]):
            patch.set_facecolor(c); patch.set_alpha(0.6)
        for i, dd in enumerate(data):
            ax.scatter(np.full(len(dd), i + 1), dd, s=18, color="#334155", zorder=3, alpha=0.7)
        ax.set_title(metric.upper()); ax.set_ylabel(metric)
    fig.suptitle(f"Experiment D — preserved vs disrupted on disrupted signals ({', '.join(disrupted_s)}) — "
                 f"'{scenario}'", fontweight="bold")
    fig.tight_layout(); out = FIG / f"expD_{scenario}.png"; fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    print(f"\n  figure -> {out}")


# --------------------------------------------------------------------------- Experiment C
def exp_c():
    print("\n=== Experiment C — performance & scalability ===")
    fp = DATA / "loadsweep.csv"
    if not fp.exists():
        print(f"  missing {fp} — run run_loadsweep.ps1 first."); return
    df = pd.read_csv(fp).sort_values("patients")
    # throughput = input samples processed per wall-second of pipeline processing
    df["throughput"] = df["produced"] / df["process_seconds"]
    df["signals"] = 5
    print(f"\n  {'patients':>8}{'produced':>12}{'proc (s)':>10}{'samples/s':>12}{'agg rows':>10}{'metrics':>9}")
    for _, r in df.iterrows():
        print(f"  {int(r.patients):>8}{int(r.produced):>12}{r.process_seconds:>10.1f}"
              f"{r.throughput:>12.0f}{int(r.agg_rows):>10}{int(r.metric_rows):>9}")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.4))
    ax1.plot(df.patients, df.process_seconds, "o-", color=COL["cwt"], lw=2, ms=7)
    ax1.set_xlabel("concurrent patients"); ax1.set_ylabel("pipeline processing time (s)")
    ax1.set_title("Processing time vs load"); ax1.set_xticks(df.patients)
    ax2.plot(df.patients, df.throughput, "s-", color=COL["cosinor"], lw=2, ms=7)
    ax2.set_xlabel("concurrent patients"); ax2.set_ylabel("throughput (samples / s)")
    ax2.set_title("Sustained throughput vs load"); ax2.set_xticks(df.patients)
    for ax in (ax1, ax2):          # pad the axes so the end markers (1 and 32 patients) sit inside the frame
        ax.margins(x=0.08, y=0.12)
    # Continue each line past the last measured point towards a linearly extrapolated (64, y) so that it
    # runs off the frame instead of appearing to end at 32 patients. The axes keep their data-driven
    # limits, so the extrapolated segment is clipped at the border and carries no marker.
    last, prev = df.iloc[-1], df.iloc[-2]
    for ax, col, color in ((ax1, "process_seconds", COL["cwt"]), (ax2, "throughput", COL["cosinor"])):
        slope = (last[col] - prev[col]) / (last.patients - prev.patients)
        y64 = last[col] + slope * (64 - last.patients)
        xl, yl = ax.get_xlim(), ax.get_ylim()
        ax.plot([last.patients, 64], [last[col], y64], "-", color=color, lw=2, zorder=1.5)
        ax.set_xlim(xl); ax.set_ylim(yl)
    fig.suptitle("Experiment C — streaming performance & scalability", fontweight="bold")
    fig.tight_layout(); out = FIG / "expC_performance.png"; fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    print(f"\n  figure -> {out}")


# --------------------------------------------------------------------------- Experiment B (multi-seed)
def _bucket(m):
    return "Cosinor" if m.startswith("Cosinor") else "CWT" if m.startswith("CWT") else "IS/IV"


def _latency_for(scenario_dir, k=K_MAIN):
    """Per-series detection latency for one snapshot dir -> list of {patient, signal, bucket, latency_h}."""
    series, onset, ev = metric_frames(scenario_dir)
    agg = load(scenario_dir, "agg_1min")
    scenario_start = agg.window_start.min()
    etype = ev.event_type.mode().iloc[0] if not ev.empty else None
    detset = DETECTORS.get(etype, {})
    out = []
    for (p, s), t_onset in onset.items():
        for label, (metric, sign) in detset.items():
            key = (p, s, metric)
            if key not in series:
                continue
            h, z = zscore_series(series[key], t_onset, metric, sign, scenario_start)
            post = h > 0
            lat = first_sustained(h[post], z[post], k) if post.any() else np.nan
            out.append(dict(patient=p, signal=s, bucket=_bucket(label), latency_h=lat))
    return out


def _boot_median_ci(a, n=3000, seed=0):
    a = np.asarray(a, float)
    rng = np.random.default_rng(seed)
    meds = [np.median(rng.choice(a, size=len(a), replace=True)) for _ in range(n)]
    return float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5))


def exp_b_stats(base_scenarios, seeds):
    """Paired CWT-vs-Cosinor detection-latency analysis pooled across seeds: per-method latency
    distributions, CWT-earlier win rate + bootstrap CI, and a paired Wilcoxon signed-rank test."""
    from scipy import stats
    print(f"\n=== Experiment B — multi-seed paired analysis (seeds {seeds}) ===")
    rows = []
    for base in base_scenarios:
        for seed in seeds:
            d = f"{base}_s{seed}"
            if not (DATA / d).exists():
                print(f"  (missing snapshot {d} — skipped)")
                continue
            for r in _latency_for(d):
                r.update(scenario=base, seed=seed)
                rows.append(r)
    df = pd.DataFrame(rows)
    if df.empty:
        print("  no multi-seed snapshots found — run run_multiseed.ps1 first.")
        return

    fig, axes = plt.subplots(1, len(base_scenarios), figsize=(5 * len(base_scenarios), 4.4), squeeze=False)
    for ax, base in zip(axes[0], base_scenarios):
        sub = df[df.scenario == base]
        piv = (sub[sub.bucket.isin(["CWT", "Cosinor"])]
               .pivot_table(index=["seed", "patient", "signal"], columns="bucket",
                            values="latency_h", aggfunc="first")
               .dropna(subset=["CWT", "Cosinor"]))
        cwt = piv["CWT"].to_numpy(); cos = piv["Cosinor"].to_numpy()
        n = len(piv); diff = cos - cwt                       # + => CWT earlier
        wins = int((diff > 0).sum())
        lo, hi = _boot_median_ci(diff)
        try:
            p = stats.wilcoxon(cwt, cos).pvalue               # two-sided paired signed-rank
        except ValueError:
            p = float("nan")
        cm = np.median(cwt); om = np.median(cos)
        print(f"\n  {base.replace('_long','')}: n = {n} paired series")
        print(f"     CWT     latency  median {cm:4.1f} h  [IQR {np.percentile(cwt,25):.1f}-{np.percentile(cwt,75):.1f}]")
        print(f"     Cosinor latency  median {om:4.1f} h  [IQR {np.percentile(cos,25):.1f}-{np.percentile(cos,75):.1f}]")
        print(f"     CWT earlier in {wins}/{n} series; median advantage {np.median(diff):+.1f} h  [95% CI {lo:.1f}, {hi:.1f}]")
        print(f"     paired Wilcoxon signed-rank  p = {p:.4g}")
        bp = ax.boxplot([cwt, cos], tick_labels=["CWT", "Cosinor"], patch_artist=True, widths=0.5)
        for patch, c in zip(bp["boxes"], [COL["cwt"], COL["cosinor"]]):
            patch.set_facecolor(c); patch.set_alpha(0.6)
        for i, a in enumerate([cwt, cos]):
            ax.scatter(np.full(len(a), i + 1) + np.random.default_rng(1).uniform(-0.06, 0.06, len(a)),
                       a, s=10, color="#334155", alpha=0.5, zorder=3)
        ax.set_title(f"{base.replace('_long','')}\n(n={n}, p={p:.3g})"); ax.set_ylabel("detection latency (h)")
    fig.suptitle(f"Experiment B — detection latency across {len(seeds)} seeds (paired CWT vs Cosinor)", fontweight="bold")
    fig.tight_layout(); out = FIG / "expB_multiseed.png"; fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    df.to_csv(FIG / "expB_multiseed_latency.csv", index=False)
    print(f"\n  figure -> {out}")


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--snapshot", metavar="SCENARIO", help="dump current Postgres output to data/<scenario>/")
    ap.add_argument("--exp", choices=["A", "B", "C", "D"], help="run an experiment on the snapshots")
    ap.add_argument("--scenario", default=None, help="scenario for A/D (default baseline / cohort)")
    ap.add_argument("--scenarios", default="amplitude_decay_long,phase_shift_long,fragmentation_long",
                    help="comma list for B")
    ap.add_argument("--bstats", action="store_true", help="Exp B multi-seed paired analysis (reads data/<scenario>_s<seed>/)")
    ap.add_argument("--seeds", default="42,43,44,45,46", help="seeds for --bstats")
    args = ap.parse_args()

    if args.snapshot:
        snapshot(args.snapshot)
    if args.exp == "A":
        exp_a(args.scenario or "baseline")
    elif args.exp == "B":
        exp_b([s.strip() for s in args.scenarios.split(",") if s.strip()])
    elif args.exp == "D":
        exp_d(args.scenario or "cohort")
    elif args.exp == "C":
        exp_c()
    if args.bstats:
        seeds = [int(s) for s in args.seeds.split(",") if s.strip()]
        exp_b_stats([s.strip() for s in args.scenarios.split(",") if s.strip()], seeds)
    if not args.snapshot and not args.exp and not args.bstats:
        ap.print_help()


if __name__ == "__main__":
    main()
