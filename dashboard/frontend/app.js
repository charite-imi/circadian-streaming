"use strict";

const SIGNAL_COLORS = {
  HR: "#0f8a5f",
  ABP_SYS: "#dc2626",
  ABP_DIA: "#991b1b",
  SPO2: "#0e7490",
  RR: "#a16207",
};
const sigColor = (s) => SIGNAL_COLORS[s] || "#1b426a";

// Typical adult normal ranges — shaded on the median so deviation is obvious at a glance.
const NORMAL_RANGES = {
  HR: [60, 100], ABP_SYS: [90, 140], ABP_DIA: [60, 90], SPO2: [94, 100], RR: [12, 20],
};

const C = {
  grid: "#e8eef5",
  line: "#cbd5e1",
  ink: "#475569",
  night: "rgba(51,65,85,0.06)",
  gt: "rgba(220,38,38,0.10)",
  ref: "rgba(21,128,61,0.10)",
  refLine: "#94a3b8",
};

const CONFIG = {
  responsive: true,
  displaylogo: false,
  modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d", "toggleSpikelines"],
  toImageButtonOptions: { format: "png", scale: 3, filename: "circadian-overview" },
};

const DAY = 86400000, HALF = 43200000;

// Plotly.js only ships Viridis/Cividis from the matplotlib family — define inferno explicitly.
const INFERNO = [
  [0.0, "#000004"], [0.11, "#1b0c41"], [0.22, "#4a0c6b"], [0.33, "#781c6d"], [0.44, "#a52c60"],
  [0.55, "#cf4446"], [0.66, "#ed6925"], [0.77, "#fb9b06"], [0.88, "#f7d03c"], [1.0, "#fcffa4"],
];
// Readable circadian period ticks (log axis) instead of Plotly's 1..9-per-decade minor-tick soup.
const PERIOD_TICKS = [0.5, 1, 2, 6, 12, 24, 48];

function yUnit(title, range) {
  const y = { gridcolor: C.grid, zeroline: false, linecolor: C.line, tickfont: { size: 11 },
    title: { text: title, font: { size: 11 } } };
  if (range) y.range = range;
  return y;
}

function baseLayout(yTitle, extra) {
  return Object.assign({
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: C.ink, size: 12, family: "Satoshi, Inter, sans-serif" },
    margin: { l: 54, r: 16, t: 6, b: 32 },
    xaxis: { type: "date", gridcolor: C.grid, zeroline: false, linecolor: C.line, tickfont: { size: 11 } },
    yaxis: yUnit(yTitle),
    showlegend: false,
    hovermode: "x unified",
    hoverlabel: {
      bgcolor: "rgba(255,255,255,0.97)",
      bordercolor: "#1b426a",
      font: { color: "#0a2043", size: 12.5, family: "Satoshi, Inter, sans-serif" },
      align: "left",
    },
  }, extra || {});
}

function nightShapes(t0ms, t1ms) {
  const first = new Date(t0ms); first.setUTCHours(18, 0, 0, 0);
  let s = first.getTime();
  if (s > t0ms) s -= DAY;
  const out = [];
  for (; s < t1ms; s += DAY) {
    out.push({ type: "rect", xref: "x", yref: "paper", x0: new Date(s).toISOString(),
      x1: new Date(s + HALF).toISOString(), y0: 0, y1: 1, fillcolor: C.night, line: { width: 0 }, layer: "below" });
  }
  return out;
}

function gtShapes(events) {
  return (events || []).map((e) => ({ type: "rect", xref: "x", yref: "paper",
    x0: e.start, x1: e.end, y0: 0, y1: 1, fillcolor: C.gt, line: { width: 0 }, layer: "below" }));
}
function gtAnnotations(events) {
  return (events || []).map((e, i) => ({
    x: e.start, y: 0.97 - (i % 3) * 0.14, xref: "x", yref: "paper", yanchor: "top", xanchor: "left",
    text: " " + e.type.replace(/_/g, " ") + " ", showarrow: false,
    font: { size: 10.5, color: "#ffffff" }, bgcolor: "rgba(185,28,28,0.92)", borderpad: 3,
  }));
}

function normalBand(signal) {
  const r = NORMAL_RANGES[signal];
  return r ? [{ type: "rect", xref: "paper", yref: "y", x0: 0, x1: 1, y0: r[0], y1: r[1],
    fillcolor: C.ref, line: { width: 0 }, layer: "below" }] : [];
}
function normalAnnotation(signal) {
  const r = NORMAL_RANGES[signal];
  return r ? [{ x: 0.995, y: r[1], xref: "paper", yref: "y", xanchor: "right", yanchor: "bottom",
    text: `normal ${r[0]}–${r[1]}`, showarrow: false, font: { size: 10, color: "#15803d" } }] : [];
}
const refLine = (yVal) => ({ type: "line", xref: "paper", yref: "y", x0: 0, x1: 1, y0: yVal, y1: yVal,
  line: { color: C.refLine, width: 1, dash: "dot" } });

function line(x, y, color, opts) {
  opts = opts || {};
  const t = {
    x, y, type: opts.gl ? "scattergl" : "scatter", mode: "lines",
    line: { color, width: 2, dash: opts.dash || "solid" }, connectgaps: false,
    hovertemplate: "%{y}<extra></extra>",
  };
  if (opts.name) t.name = opts.name;
  return t;
}

// --- data helpers ---
const col = (arr, k) => arr.map((d) => d[k]);
function lastNum(arr, k) {
  for (let i = arr.length - 1; i >= 0; i--) {
    const v = arr[i][k];
    if (v !== null && v !== undefined && !Number.isNaN(v)) return v;
  }
  return null;
}
function peakHour(acroRad) {
  if (acroRad === null) return null;
  let h = (-acroRad * 24) / (2 * Math.PI);
  return ((h % 24) + 24) % 24;
}
const fmt = (v, d) => (v === null ? "—" : v.toFixed(d));
function hhmm(h) {
  if (h === null) return "—";
  const m = Math.round(h * 60);
  return String(Math.floor(m / 60) % 24).padStart(2, "0") + ":" + String(m % 60).padStart(2, "0");
}

const median = (a) => { const s = [...a].sort((x, y) => x - y); return s.length ? s[Math.floor(s.length / 2)] : null; };

// --- rhythm status ---
function rhythmStatus(o) {
  const amps = o.cosinor.map((d) => d.amplitude).filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  const r2 = lastNum(o.cosinor, "r2");
  const cri = lastNum(o.cri, "cri");
  if (!amps.length && r2 === null && cri === null) {
    return { cls: "none", label: "No rhythm metrics", reason: "insufficient data in the analysis window" };
  }

  const fit = r2 !== null ? r2 : cri;
  const fitName = r2 !== null ? "R²" : "CRI";

  // (1) No coherent rhythm at all — the cosine explains almost none of the variance.
  if (fit !== null && fit < 0.3) {
    return { cls: "crit", label: "Rhythm disrupted",
      reason: `weak fit (${fitName} ${fit.toFixed(2)}) — little coherent 24 h rhythm.` };
  }

  // (2) Amplitude declined from the record's own earlier baseline.
  if (amps.length >= 8) {
    const third = Math.max(3, Math.floor(amps.length / 3));
    const base = median(amps.slice(0, third));
    const curr = median(amps.slice(-Math.max(3, Math.floor(amps.length / 6))));
    const decline = base > 0 ? 1 - curr / base : 0;
    if (decline >= 0.4) {
      const pct = Math.round(decline * 100);
      const strong = decline >= 0.65;
      return { cls: strong ? "crit" : "warn", label: strong ? "Rhythm disrupted" : "Rhythm weakened",
        reason: `amplitude fell ~${pct}% (${base.toFixed(1)} → ${curr.toFixed(1)}) across the record.` };
    }
  }

  // (3) Present but modest — the fit explains a minority of the variance (matches the "good ≥ 0.5" line on the R² panel).
  if (fit !== null && fit < 0.5) {
    return { cls: "warn", label: "Rhythm weak",
      reason: `modest fit (${fitName} ${fit.toFixed(2)}) — a low-amplitude / partial 24 h rhythm.` };
  }

  // (4) Clear, stable rhythm.
  const basis = r2 !== null ? `Cosinor R² ${r2.toFixed(2)}` : (cri !== null ? `CRI ${cri.toFixed(2)}` : "");
  return { cls: "good", label: "Rhythm preserved", reason: `stable amplitude${basis ? ", " + basis : ""} — a clear 24 h rhythm.` };
}
const ICONS = {
  good: '<svg class="ic" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M5 10.5l3.2 3.2L15 6.5" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  warn: '<svg class="ic" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M10 3.5L18.5 16.5H1.5L10 3.5z" stroke-linejoin="round"/><path d="M10 8.5v3.4" stroke-linecap="round"/></svg>',
  crit: '<svg class="ic" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="10" cy="10" r="7"/><path d="M10 6v5" stroke-linecap="round"/></svg>',
  none: "",
};
function renderStatus(o) {
  const s = rhythmStatus(o);
  const el = document.getElementById("status");
  el.hidden = o.median.length === 0;
  el.className = "status-banner " + s.cls;
  el.innerHTML = `<span class="badge">${ICONS[s.cls] || ""}${s.label}</span><span class="reason">${s.reason}</span>`;
}

// --- stat tiles ---
function renderStats(o) {
  let span = "—";
  if (o.median.length) {
    span = ((new Date(o.median[o.median.length - 1].t) - new Date(o.median[0].t)) / DAY).toFixed(1) + " d";
  }
  const tiles = [
    ["Cosinor amplitude", fmt(lastNum(o.cosinor, "amplitude"), 2)],
    ["Goodness-of-fit R²", fmt(lastNum(o.cosinor, "r2"), 2)],
    ["Acrophase (UTC)", hhmm(peakHour(lastNum(o.cosinor, "acrophase")))],
    ["CWT CRI", fmt(lastNum(o.cri, "cri"), 2)],
    ["IS · IV", fmt(lastNum(o.isiv, "is"), 2) + " · " + fmt(lastNum(o.isiv, "iv"), 2)],
    ["Data span", span],
  ];
  document.getElementById("stats").innerHTML = tiles
    .map(([l, v]) => `<div class="stat"><div class="label">${l}</div><div class="value">${v}</div></div>`)
    .join("");
}

function renderMedian(o, color) {
  if (!o.median.length) { Plotly.purge("median"); return; }
  const x = col(o.median, "t"), y = col(o.median, "value");
  const t0 = +new Date(x[0]), t1 = +new Date(x[x.length - 1]);
  const layout = baseLayout("value", {
    shapes: normalBand(o.signal).concat(nightShapes(t0, t1)).concat(gtShapes(o.ground_truth)),
    annotations: normalAnnotation(o.signal).concat(gtAnnotations(o.ground_truth)),
  });
  // Pin the axis to the data. Autorange would also fit shapes and labels, and a scenario's far-future
  // "never recovers" end date would squash the chart.
  layout.xaxis.range = [x[0], x[x.length - 1]];
  Plotly.react("median", [line(x, y, color, { gl: true })], layout, CONFIG);
}

// Cone-of-influence overlay: dim the region above the COI boundary (long periods near the window
// edges), where the wavelet result is edge-contaminated and must not be trusted. The boundary is a
// crisp dotted line so clinicians can read exactly where results become reliable.
function coiTraces(sc) {
  if (!sc.coi || !sc.coi.length) return [];
  const ymax = sc.periods[sc.periods.length - 1];
  const xBack = sc.times.slice().reverse();
  return [
    {
      type: "scatter", mode: "lines",
      x: sc.times.concat(xBack),
      y: sc.coi.map((c) => Math.min(c, ymax)).concat(sc.times.map(() => ymax)),
      fill: "toself", fillcolor: "rgba(8,11,20,0.62)",
      line: { width: 0 }, hoverinfo: "skip", showlegend: false,
    },
    {
      type: "scatter", mode: "lines", x: sc.times, y: sc.coi,
      line: { color: "rgba(255,255,255,0.82)", width: 1.2, dash: "dot" },
      hoverinfo: "skip", showlegend: false,
    },
  ];
}

function renderScalogram(sc) {
  if (!sc || !sc.periods.length) { Plotly.purge("scalogram"); return; }
  const trace = {
    type: "heatmap", z: sc.power, x: sc.times, y: sc.periods,
    colorscale: INFERNO,
    colorbar: { title: { text: "power", side: "right", font: { size: 11 } }, thickness: 12, tickfont: { size: 10 } },
    hovertemplate: "%{x}<br>period %{y:.1f} h<br>power %{z:.3g}<extra></extra>",
  };
  const layout = baseLayout("period (h)", {
    yaxis: { type: "log", tickmode: "array", tickvals: PERIOD_TICKS,
      ticktext: PERIOD_TICKS.map((h) => h + " h"),
      gridcolor: C.grid, linecolor: C.line, tickfont: { size: 11 },
      title: { text: "period" } },
    hovermode: "closest",
    shapes: [
      { type: "line", xref: "paper", yref: "y", x0: 0, x1: 1, y0: sc.band[0], y1: sc.band[0], line: { color: "#e11d48", width: 1, dash: "dot" } },
      { type: "line", xref: "paper", yref: "y", x0: 0, x1: 1, y0: sc.band[1], y1: sc.band[1], line: { color: "#e11d48", width: 1, dash: "dot" } },
    ],
    annotations: [
      { xref: "paper", yref: "paper", x: 0.012, y: 0.98, xanchor: "left", yanchor: "top",
        text: "shaded = cone of influence (edge-affected — not reliable)", showarrow: false,
        font: { size: 10, color: "rgba(255,255,255,0.92)" },
        bgcolor: "rgba(8,11,20,0.55)", borderpad: 3 },
    ],
  });
  Plotly.react("scalogram", [trace].concat(coiTraces(sc)), layout, CONFIG);
}

function renderLightTherapy(lt) {
  if (!lt || !lt.available) { Plotly.purge("lighttherapy"); return; }
  const trace = {
    x: lt.steps.map((s) => s.hour),
    y: lt.steps.map((s) => s.intensity),
    type: "bar",
    marker: { color: lt.steps.map((s) => s.rgb), line: { width: 0 } },
    width: (24 / lt.steps.length) * 0.96,
    customdata: lt.steps.map((s) => s.cct),
    hovertemplate: "%{x:.1f} h<br>intensity %{y:.2f}<br>%{customdata} K<extra></extra>",
  };
  const layout = baseLayout("intensity", {
    xaxis: { title: { text: "hour of day (UTC)", font: { size: 11 } }, range: [0, 24], dtick: 3,
      gridcolor: C.grid, zeroline: false, linecolor: C.line, tickfont: { size: 11 } },
    yaxis: yUnit("intensity", [0, 1]),
    hovermode: "x",
    bargap: 0,
    shapes: [{ type: "line", xref: "x", yref: "paper", x0: lt.peak_hour, x1: lt.peak_hour, y0: 0, y1: 1,
      line: { color: "#1b426a", width: 1.5, dash: "dash" } }],
    annotations: [{ x: lt.peak_hour, y: 1, xref: "x", yref: "paper", yanchor: "bottom",
      text: ` peak ${hhmm(lt.peak_hour)}`, showarrow: false, font: { size: 10, color: "#1b426a" } }],
  });
  Plotly.react("lighttherapy", [trace], layout, CONFIG);
}

function renderSeries(id, traces, yTitle, extra) {
  Plotly.react(id, traces, baseLayout(yTitle, extra), CONFIG);
}

function renderOverview(o, sc, lt) {
  document.getElementById("empty").hidden = o.median.length > 0;
  const color = sigColor(o.signal);
  renderStatus(o);
  renderStats(o);
  renderMedian(o, color);
  renderScalogram(sc);
  renderLightTherapy(lt);

  const cx = col(o.cosinor, "t");
  renderSeries("amp", [line(cx, col(o.cosinor, "amplitude"), color)], "amplitude");
  renderSeries("r2", [line(cx, col(o.cosinor, "r2"), color)], "R²",
    { yaxis: yUnit("R²", [0, 1]), shapes: [refLine(0.5)],
      annotations: [{ x: 0.01, y: 0.5, xref: "paper", yref: "y", xanchor: "left", yanchor: "bottom",
        text: " good ≥ 0.5", showarrow: false, font: { size: 10, color: C.ink } }] });
  renderSeries("cri", [line(col(o.cri, "t"), col(o.cri, "cri"), color)], "CRI", { yaxis: yUnit("CRI", [0, 1]) });

  const ix = col(o.isiv, "t");
  renderSeries("isiv", [
    line(ix, col(o.isiv, "is"), color, { name: "IS (stability)" }),
    line(ix, col(o.isiv, "iv"), color, { dash: "dash", name: "IV (fragmentation)" }),
  ], "value", { showlegend: true, legend: { orientation: "h", y: 1.16, x: 0, font: { size: 11 } } });
}

// --- wiring ---
let PATIENTS = [];
let lastUpdate = null;
const POLL_MS = 45000;

async function refresh(silent) {
  const patient = document.getElementById("patient").value;
  const signal = document.getElementById("signal").value;
  if (!patient || !signal) return;
  const loading = document.getElementById("loading");
  if (!silent) loading.classList.add("show");
  try {
    const q = `patient=${encodeURIComponent(patient)}&signal=${encodeURIComponent(signal)}`;
    const [o, sc, lt] = await Promise.all([
      fetch(`/api/overview?${q}`).then((r) => r.json()),
      fetch(`/api/scalogram?${q}`).then((r) => r.json()),
      fetch(`/api/lighttherapy?${q}`).then((r) => r.json()),
    ]);
    renderOverview(o, sc, lt);
    lastUpdate = Date.now();
    tickFreshness();
  } catch (err) {
    console.error("refresh failed", err);
  } finally {
    if (!silent) loading.classList.remove("show");
  }
}

// "updated Xs ago" indicator
function tickFreshness() {
  const el = document.getElementById("freshness");
  const dot = document.getElementById("freshdot");
  if (!el) return;
  if (!lastUpdate) { el.textContent = "loading…"; return; }
  const secs = Math.round((Date.now() - lastUpdate) / 1000);
  el.textContent = secs < 60 ? `updated ${secs}s ago` : `updated ${Math.floor(secs / 60)}m ago`;
  if (dot) dot.classList.toggle("stale", secs > 120);
}

function fillSignals() {
  const p = PATIENTS.find((x) => x.patient_id === document.getElementById("patient").value);
  const sel = document.getElementById("signal");
  const prev = sel.value;
  sel.innerHTML = (p ? p.signals : []).map((s) => `<option>${s}</option>`).join("");
  if (p && p.signals.includes(prev)) sel.value = prev;
  else if (p && p.signals.includes("HR")) sel.value = "HR";
}

async function init() {
  PATIENTS = await fetch("/api/patients").then((r) => r.json());
  const psel = document.getElementById("patient");
  psel.innerHTML = PATIENTS.map((p) => `<option>${p.patient_id}</option>`).join("");
  psel.addEventListener("change", () => { fillSignals(); refresh(); });
  document.getElementById("signal").addEventListener("change", () => refresh());
  if (PATIENTS.length) { fillSignals(); refresh(); }
  else document.getElementById("empty").hidden = false;

  setInterval(() => {
    if (document.visibilityState === "visible" && document.getElementById("patient").value) {
      refresh(true);
    }
  }, POLL_MS);
  setInterval(tickFreshness, 1000);
}

init();