"""Reproduce the 'healthy rhythm' vs 'decaying rhythm' scalogram figure from the demo_decay scenario.
Same complex-Morlet wavelet as the pipeline/dashboard; both panels share one colour scale so the decay
reads as a dimming band. Run after `run_scenario.ps1 -Scenario demo_decay`. -> figures/demo_scalogram.png
"""
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg2
import pywt

DSN = dict(host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5544")),
           dbname="circadian", user="circadian", password="circadian")
DT = 1.0 / 60.0
WAVELET = "cmor2.0-0.9549"
MIN_P, MAX_P, VOICES = 6.0, 96.0, 12          # geometric-centre period = 24 h
BG = "#f5f1e6"                                  # cream background like the target


def scalogram(patient, signal="HR"):
    conn = psycopg2.connect(**DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT median FROM agg_1min WHERE patient_id=%s AND signal_type=%s "
                    "ORDER BY window_start", (patient, signal))
        vals = np.array([r[0] for r in cur.fetchall()], float)
    conn.close()
    vals = vals - np.nanmean(vals)
    n_scales = int(np.floor(VOICES * np.log2(MAX_P / MIN_P))) + 1
    periods = MIN_P * 2.0 ** (np.arange(n_scales) / VOICES)
    scales = periods * pywt.central_frequency(WAVELET) / DT
    coef, _ = pywt.cwt(vals, scales, WAVELET, sampling_period=DT)
    return np.abs(coef) ** 2, periods


hp, periods = scalogram("P0001")               # healthy
dp, _ = scalogram("P0002")                      # decaying
vmax = np.percentile(hp, 99.5)                   # shared scale from the healthy panel

fig, axes = plt.subplots(1, 2, figsize=(11, 3.4), facecolor=BG)
for ax, z, title in [(axes[0], hp, "healthy rhythm"), (axes[1], dp, "decaying rhythm")]:
    ax.imshow(z, aspect="auto", origin="lower", cmap="inferno", vmin=0, vmax=vmax,
              extent=[0, z.shape[1], 0, z.shape[0]])
    ax.set_title(title, fontsize=12, family="monospace", color="#5b5b5b", pad=10)
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
fig.patch.set_facecolor(BG)
fig.tight_layout()
out = os.path.join(os.path.dirname(__file__), "figures", "demo_scalogram.png")
fig.savefig(out, dpi=160, facecolor=BG, bbox_inches="tight")
print("figure ->", out)
