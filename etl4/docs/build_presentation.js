const pptxgen = require("pptxgenjs");
const path = require("path");

const FIG = "B:/IISC/DE/Proj/etl4/docs/figures";

// Palette and metrics lifted from RL_Presentation_Final_1 (1) (2).pptx
const THEME = {
  name: "ETL4 ADS-B",
  headFontFace: "Arial",
  bodyFontFace: "Arial",
  colors: {
    dk1: "333333", lt1: "FFFFFF",
    dk2: "002D56", lt2: "E8EEF5",
    accent1: "002D56", accent2: "C8501E", accent3: "2EA343",
    accent4: "BFC8D6", accent5: "666666", accent6: "C00000",
    hlink: "002D56", folHlink: "666666",
  },
};
const NAVY = "002D56", ORANGE = "C8501E", BODY = "333333",
      MUTED = "666666", PALE = "E8EEF5", RULE = "BFC8D6",
      GREEN = "2EA343", RED = "C00000", WHITE = "FFFFFF",
      GREY = "F7F8FA", WARM = "F6E7E0", MINT = "EAF4EC";

const X = 0.7, W = 11.93;
const TOP = 1.16;          // first content baseline
const BOT = 6.84;          // nothing below this on a CONTENT slide

/* ------------------------------------------------- text measurement ----- */
// Mirrors audit.py so the generator and the checker agree.
const AVG_CHAR_W = 0.50, LINE_H = 1.21;

function wrapLines(text, size, widthIn) {
  if (!text) return 1;
  const charW = (size * AVG_CHAR_W) / 72;
  const perLine = Math.max(1, Math.floor(widthIn / charW));
  let line = 0, n = 1;
  for (const word of String(text).split(/\s+/).filter(Boolean)) {
    const add = word.length + (line ? 1 : 0);
    if (line + add > perLine) { n += 1; line = word.length; }
    else { line += add; }
  }
  return n;
}
function textH(text, size, widthIn, lh) {
  lh = lh || LINE_H;
  let n = 0;
  for (const para of String(text).split("\n")) {
    n += para.trim() === "" ? 1 : wrapLines(para, size, widthIn);
  }
  return (n * size * lh) / 72;
}

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";            // 13.333 x 7.5
pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
pres.author = "Team ETL4";
pres.company = "Indian Institute of Science, Bangalore";
pres.subject = "DA 231o Data Engineering at Scale";
pres.title = "Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data";

/* ---------------------------------------------------------------- layouts */

pres.defineSlideMaster({
  title: "TITLE",
  background: { color: NAVY },
  objects: [
    { rect: { x: 0, y: 5.55, w: 13.333, h: 1.95, fill: { color: "001B35" } } },
    { placeholder: {
        options: { name: "eyebrow", type: "body", x: X, y: 1.05, w: W, h: 0.4,
                   fontSize: 16, color: RULE, charSpacing: 2, bold: true, isTextBox: true },
        text: "" } },
    { placeholder: {
        options: { name: "title", type: "title", x: X, y: 1.60, w: 11.6, h: 2.05,
                   fontSize: 36, bold: true, color: WHITE, valign: "top" },
        text: "" } },
    { placeholder: {
        options: { name: "subtitle", type: "body", x: X, y: 3.80, w: 11.6, h: 1.1,
                   fontSize: 18, color: RULE, isTextBox: true },
        text: "" } },
    { placeholder: {
        options: { name: "body", type: "body", x: X, y: 5.85, w: W, h: 1.35,
                   fontSize: 15, color: "C7D2E0", isTextBox: true },
        text: "" } },
  ],
});

pres.defineSlideMaster({
  title: "CONTENT",
  background: { color: WHITE },
  objects: [
    { line: { x: 0, y: 6.98, w: 13.333, h: 0, line: { color: RULE, width: 1 } } },
    { text: { text: "ETL4  |  DA 231o Data Engineering at Scale",
              options: { x: X, y: 7.04, w: 7.0, h: 0.34, fontSize: 10, color: MUTED,
                         isTextBox: true, margin: 0, valign: "middle" } } },
    { placeholder: {
        options: { name: "title", type: "title", x: X, y: 0.38, w: W, h: 0.62,
                   fontSize: 30, bold: true, color: NAVY, valign: "middle", margin: 0 },
        text: "" } },
    { placeholder: {
        options: { name: "body", type: "body", x: X, y: TOP, w: W, h: 5.4,
                   fontSize: 17, color: BODY, isTextBox: true },
        text: "" } },
    { placeholder: {
        options: { name: "chart", type: "chart", x: X, y: TOP, w: W, h: 4.0,
                   color: BODY },
        text: "" } },
    { slideNumber: { x: 11.83, y: 7.04, w: 0.8, h: 0.34, fontSize: 11,
                     color: MUTED, align: "right", margin: 0 } },
  ],
});

pres.defineSlideMaster({
  title: "SECTION",
  background: { color: PALE },
  objects: [
    { line: { x: 0, y: 6.98, w: 13.333, h: 0, line: { color: RULE, width: 1 } } },
    { placeholder: {
        options: { name: "eyebrow", type: "body", x: X, y: 2.55, w: W, h: 0.42,
                   fontSize: 16, bold: true, color: ORANGE, charSpacing: 2, isTextBox: true },
        text: "" } },
    { placeholder: {
        options: { name: "title", type: "title", x: X, y: 3.02, w: 11.0, h: 0.95,
                   fontSize: 40, bold: true, color: NAVY, valign: "middle", margin: 0 },
        text: "" } },
    { placeholder: {
        options: { name: "body", type: "body", x: X, y: 4.05, w: 10.2, h: 1.1,
                   fontSize: 17, color: BODY, isTextBox: true },
        text: "" } },
    { slideNumber: { x: 11.83, y: 7.04, w: 0.8, h: 0.34, fontSize: 11,
                     color: MUTED, align: "right", margin: 0 } },
  ],
});

/* ------------------------------------------------------------- primitives */

let uid = 0;
const nm = (p) => p + "-" + (++uid);

// Lead sentence under the title. Returns the y where the next block may start.
function lead(slide, text, opts) {
  const o = opts || {};
  const size = o.size || 17;
  const w = o.w || W;
  const h = textH(text, size, w - 0.1) + 0.04;
  slide.addText(text, {
    x: o.x || X, y: TOP - 0.02, w: w, h: h, fontSize: size, color: BODY,
    isTextBox: true, margin: 0, objectName: nm("lead"),
  });
  return TOP - 0.02 + h + (o.gap === undefined ? 0.26 : o.gap);
}

const PADX = 0.22, PADT = 0.15, PADB = 0.17, HEAD_GAP = 0.07;
// The auditor measures a text box against (w - 0.1); match that so the two agree.
const SLACK = 0.1;

function headH(c, w) {
  return textH(c.head, c.headSize || 15.5, w - 2 * PADX - SLACK);
}
function bodyH(c, w) {
  return textH(c.body, c.bodySize || 13.5, w - 2 * PADX - SLACK);
}
function cardHeight(c, w) {
  return PADT + headH(c, w) + HEAD_GAP + bodyH(c, w) + PADB;
}

function drawCard(slide, c, x, y, w, h) {
  const inner = w - 2 * PADX;
  const hh = headH(c, w);
  slide.addShape(pres.ShapeType.roundRect, {
    x: x, y: y, w: w, h: h, rectRadius: 0.06,
    fill: { color: c.fill || PALE }, line: { color: c.fill || PALE, width: 0 },
    objectName: nm("cardbg"),
  });
  slide.addText(c.head, {
    x: x + PADX, y: y + PADT, w: inner, h: hh, fontSize: c.headSize || 15.5, bold: true,
    color: c.headColor || NAVY, isTextBox: true, margin: 0, objectName: nm("cardh"),
  });
  slide.addText(c.body, {
    x: x + PADX, y: y + PADT + hh + HEAD_GAP, w: inner,
    h: h - PADT - hh - HEAD_GAP - PADB + 0.02,
    fontSize: c.bodySize || 13.5, color: c.bodyColor || BODY, isTextBox: true, margin: 0,
    objectName: nm("cardb"),
  });
}

// Vertical stack of cards in a column, each sized to its own content.
// Throws if the stack cannot fit above `bottom`, so overflow is caught at build time.
function fitColumn(slide, cards, x, y, w, bottom, gap) {
  gap = gap === undefined ? 0.18 : gap;
  const hs = cards.map((c) => cardHeight(c, w));
  const need = hs.reduce((a, b) => a + b, 0) + gap * (cards.length - 1);
  const avail = bottom - y;
  if (need > avail + 0.001) {
    throw new Error(`fitColumn overflow: needs ${need.toFixed(2)}in, has ${avail.toFixed(2)}in ` +
                    `[first head: ${cards[0].head.split("\n")[0]}]`);
  }
  const slack = (avail - need) / Math.max(1, cards.length - 1);
  let cy = y;
  cards.forEach((c, i) => {
    drawCard(slide, c, x, cy, w, hs[i]);
    cy += hs[i] + gap + (i < cards.length - 1 ? slack : 0);
  });
  return bottom;
}

// A row of equal-height cards. Returns the y below the row.
function cardRow(slide, y, cards, opts) {
  const o = opts || {};
  const gap = o.gap === undefined ? 0.22 : o.gap;
  const total = o.w || W;
  const cw = (total - gap * (cards.length - 1)) / cards.length;
  const h = o.h || Math.max(...cards.map((c) => cardHeight(c, cw)));
  cards.forEach((c, i) => drawCard(slide, c, (o.x || X) + i * (cw + gap), y, cw, h));
  return y + h + (o.after === undefined ? 0.24 : o.after);
}

// One card at an explicit box. Returns y below it.
function cardAt(slide, c, x, y, w, h) {
  const hh = h || cardHeight(c, w);
  drawCard(slide, c, x, y, w, hh);
  return y + hh;
}

// "Setup: ... / Result: ..." footnote, anchored to the bottom of the content area.
function footnote(slide, lines) {
  const inner = W - SLACK;
  const h = lines.reduce((a, l) => a + textH(l, 12.5, inner), 0) + 0.04;
  const runs = [];
  lines.forEach((ln, i) => {
    const sep = ln.indexOf(":");
    runs.push({ text: ln.slice(0, sep + 1), options: { bold: true, color: NAVY } });
    runs.push({ text: ln.slice(sep + 1), options: { color: MUTED, breakLine: i < lines.length - 1 } });
  });
  slide.addText(runs, {
    x: X, y: BOT - h, w: W, h: h, fontSize: 12.5, color: MUTED,
    isTextBox: true, margin: 0, objectName: nm("footnote"),
  });
  return BOT - h;   // top of the footnote: content must end above this
}

function stat(slide, o) {
  const inner = o.w - 0.36;
  const vSize = o.size || 34;
  const vH = (vSize * LINE_H) / 72;
  const lH = textH(o.label, 12.5, inner - SLACK);
  const h = o.h || 0.18 + vH + 0.06 + lH + 0.18;
  slide.addShape(pres.ShapeType.roundRect, {
    x: o.x, y: o.y, w: o.w, h: h, rectRadius: 0.06,
    fill: { color: o.fill || PALE }, line: { color: o.fill || PALE, width: 0 },
    objectName: nm("statbg"),
  });
  slide.addText(o.value, {
    x: o.x + 0.18, y: o.y + 0.16, w: inner, h: vH + 0.06, fontSize: vSize, bold: true,
    color: o.color || NAVY, isTextBox: true, margin: 0, objectName: nm("statv"),
  });
  slide.addText(o.label, {
    x: o.x + 0.18, y: o.y + 0.18 + vH + 0.06, w: inner, h: lH + 0.04, fontSize: 12.5,
    color: o.labelColor || MUTED, isTextBox: true, margin: 0, objectName: nm("statl"),
  });
  return o.y + h;
}

function statRow(slide, y, items, opts) {
  const o = opts || {};
  const gap = o.gap === undefined ? 0.22 : o.gap;
  const total = o.w || W;
  const cw = (total - gap * (items.length - 1)) / items.length;
  const size = o.size || 34;
  const h = Math.max(...items.map((it) =>
    0.18 + (size * LINE_H) / 72 + 0.06 + textH(it.label, 12.5, cw - 0.36 - SLACK) + 0.18));
  items.forEach((it, i) => stat(slide, Object.assign({}, it, {
    x: (o.x || X) + i * (cw + gap), y: y, w: cw, h: h, size: size,
    fill: it.fill || o.fill, color: it.color || o.color,
  })));
  return y + h + (o.after === undefined ? 0.22 : o.after);
}

function bullets(slide, items, o) {
  const w = o.w || W, size = o.fontSize || 15;
  const h = items.reduce((a, t) => a + textH(t, size, w - 0.45) + 0.12, 0);
  slide.addText(items.map((t, i) => ({
    text: t, options: { bullet: true, breakLine: i < items.length - 1 },
  })), Object.assign({ x: X, w: w, h: h, fontSize: size, color: BODY,
    isTextBox: true, margin: 0, paraSpaceAfter: 8, objectName: nm("bullets") }, o));
  return (o.y || TOP) + h;
}

function kicker(slide, text, x, y, w) {
  slide.addText(text, { x: x, y: y, w: w, h: 0.26, fontSize: 12.5, bold: true,
    color: ORANGE, charSpacing: 1.5, isTextBox: true, margin: 0, objectName: nm("kicker") });
  return y + 0.34;
}

function tableRows(rows, cfg) {
  return rows.map((r, i) => r.map((c, j) => {
    let col = BODY;
    if (i > 0 && cfg && cfg.colorFn) col = cfg.colorFn(i, j, c) || BODY;
    return { text: c, options: {
      bold: i === 0 || (i > 0 && col !== BODY),
      color: i === 0 ? WHITE : col,
      fontSize: i === 0 ? ((cfg && cfg.headSize) || 11.5) : ((cfg && cfg.size) || 12.5),
      fill: { color: i === 0 ? NAVY : (i % 2 ? WHITE : GREY) },
      align: cfg && cfg.alignFn ? cfg.alignFn(j) : "left",
      valign: "middle",
    } };
  }));
}

const chartBase = {
  catAxisLabelColor: MUTED, valAxisLabelColor: MUTED,
  catAxisLabelFontSize: 12, valAxisLabelFontSize: 12,
  catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt",
  dataLabelFontFace: "+mn-lt", dataLabelFontSize: 11,
  valGridLine: { color: "E3E7EC", size: 1 },
  catGridLine: { style: "none" },
  titleFontFace: "+mn-lt", titleFontSize: 13, titleColor: NAVY,
  legendFontFace: "+mn-lt", legendFontSize: 11.5, legendColor: MUTED,
  valAxisTitleFontSize: 11.5, valAxisTitleColor: MUTED, valAxisTitleFontFace: "+mn-lt",
};

/* ------------------------------------------------------------------ deck */

/* 1 ------------------------------------------------------------- title */
pres.addSection({ title: "Opening" });
let s = pres.addSlide({ masterName: "TITLE", sectionTitle: "Opening" });
s.addText("DA 231o  PROJECT PRESENTATION", { placeholder: "eyebrow" });
s.addText("Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data",
  { placeholder: "title" });
s.addText("Accuracy and cost trade-offs at scale, measured on 121.6 million OpenSky observations",
  { placeholder: "subtitle" });
s.addText([
  { text: "Team ETL4", options: { bold: true, color: WHITE, breakLine: true } },
  { text: "Kompalli Avinash Bhargav  |  Nadhiya R S  |  Kasi Annapurna Uppalapati  |  Sathyanarayanan C",
    options: { breakLine: true } },
  { text: "Data Engineering at Scale, Indian Institute of Science, Bangalore  |  November 2026" },
], { placeholder: "body" });
s.addNotes("Course project for DA 231o. The deliverable is a working lakehouse pipeline plus a measured comparison of three trajectory reconstruction methods.");

/* 2 ------------------------------------------- problem: not trajectories */
pres.addSection({ title: "Problem" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
s.addText("ADS-B gives us position reports, not flights", { placeholder: "title" });
let y = lead(s, "Aircraft broadcast a position roughly once per second. OpenSky aggregates those broadcasts from several thousand crowdsourced receivers into one interleaved stream with no flight boundaries marked.");
cardRow(s, y, [
  { head: "1.  Order the stream",
    body: "Group reports by aircraft and sort by the time the observation was made. Reports arrive out of order, so arrival time cannot be trusted as event time." },
  { head: "2.  Find the boundaries",
    body: "Decide where one flight ends and the next begins. One airframe flies several distinct flights in a day, and the raw stream does not distinguish them." },
  { head: "3.  Repair the path",
    body: "Make the trajectory continuous and physically plausible. Points go missing, overlapping receivers duplicate them, and some values are corrupt." },
]);
footnote(s, [
  "Source: OpenSky Network monthly flight-record archives 2019 to 2022, daily state-vector archives, and the live REST feed.",
  "Scope: convert that stream into trajectories at scale, implement three strategies for the boundary problem, and measure the accuracy and the cost of each.",
]);
s.addNotes("The core point: the raw stream is not the deliverable. Everything downstream exists to turn it into flights.");

/* 3 --------------------------------------------- motivation + question */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
s.addText("Why the standard time-gap rule is not enough", { placeholder: "title" });
y = lead(s, "The usual rule splits a flight whenever two consecutive observations are more than thirty minutes apart. It looks only at elapsed time, never at the motion those two observations imply.");
y = cardRow(s, y, [
  { fill: WARM, headColor: ORANGE, head: "A long gap is not always a new flight",
    body: "An aircraft parked forty minutes between legs and an aircraft that loses receiver coverage for forty minutes while cruising produce an identical gap. The rule splits both. One of those splits is wrong." },
  { fill: WARM, headColor: ORANGE, head: "A short gap is not always continuous",
    body: "Two reports three seconds apart implying a 200 km jump are physically impossible and almost certainly corrupt. The rule accepts them, because the time gap is small." },
]);
s.addShape(pres.ShapeType.roundRect, { x: X, y: y + 0.1, w: W, h: 1.25, rectRadius: 0.06,
  fill: { color: NAVY }, line: { color: NAVY, width: 0 }, objectName: "rq-bg" });
s.addText([
  { text: "RESEARCH QUESTION", options: { fontSize: 11.5, bold: true, color: RULE, charSpacing: 2, breakLine: true } },
  { text: "How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?",
    options: { fontSize: 16.5, color: WHITE, italic: true } },
], { x: X + 0.3, y: y + 0.24, w: W - 0.6, h: 0.98, isTextBox: true, margin: 0,
     lineSpacingMultiple: 1.18, objectName: "rq" });
footnote(s, ["Resolution: kinematics address both cases, since speed, heading change, altitude rate and turn rate decide whether the implied motion is achievable for the aircraft class."]);

/* 4 ------------------------------------------------ goals and targets */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
s.addText("Design goals and performance targets", { placeholder: "title" });
const colW = (W - 0.3) / 2, colR = X + colW + 0.3;
kicker(s, "SEVEN DESIGN GOALS", X, TOP - 0.02, colW);
bullets(s, [
  "Ingest historical archives and a live streaming feed",
  "Handle missing, duplicated, irregular and out-of-order records",
  "Reconstruct with a time baseline and two kinematic methods",
  "Extract speed, heading, acceleration, turn and altitude rate",
  "Detect significant deviations from a reference trajectory",
  "Generate a smoothed trajectory where the input is noisy",
  "Measure accuracy and cost together, not in isolation",
], { x: X, y: TOP + 0.34, w: colW, fontSize: 14.5 });

kicker(s, "SIX PERFORMANCE TARGETS", colR, TOP - 0.02, colW);
bullets(s, [
  "Throughput in records per second",
  "End-to-end processing time and streaming latency",
  "CPU and memory utilisation",
  "Shuffle and partitioning overhead",
  "Scaling efficiency as data volume grows",
  "Robustness to missing, duplicated and out-of-order data",
], { x: colR, y: TOP + 0.34, w: colW, fontSize: 14.5 });

const fnTop4 = footnote(s, ["Constraints: every stage is reproducible under a fixed seed, and every schema or parameter change is recorded in an append-only decision log."]);
const c4 = { fill: PALE, head: "Target 5 drove the architecture",
  body: "Scaling efficiency asks whether cost grows with new data or with accumulated data. A pipeline that rebuilds everything each run costs the square of the dataset, and no per-stage optimisation fixes that. Everything in the next section follows from this." };
cardAt(s, c4, colR, fnTop4 - 0.2 - cardHeight(c4, colW), colW);

/* 5 -------------------------------------------------------- architecture */
pres.addSection({ title: "Approach" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Pipeline architecture", { placeholder: "title" });
y = lead(s, "Seven stages in a linear flow. Each tier persists its output, so a downstream tier can be rebuilt without re-running the work above it.");

const tiers = [
  { t: "OpenSky Network", d: "monthly archives\nlive REST feed\ndaily state archives", c: "F0F2F5", tc: BODY },
  { t: "Apache Kafka", d: "raw topic, 3 partitions\ndead-letter topic\nkey = airframe id", c: "F0F2F5", tc: BODY },
  { t: "Bronze", d: "source schema kept\nno projection\n121.6 M rows, 6.2 GB", c: PALE, tc: NAVY },
  { t: "Silver", d: "Data Vault 2.0\nhubs, links, satellites\n225.5 M rows, 15 GB", c: PALE, tc: NAVY },
  { t: "Gold", d: "dimensional marts\nfacts and dimensions\n19.0 M rows, 2.0 GB", c: PALE, tc: NAVY },
  { t: "Experiments", d: "3 methods x 4 profiles\n12 cells, two inputs\nPostgres + Grafana", c: "FBEDE6", tc: ORANGE },
];
const cw6 = 1.80, gap6 = 0.226, tierH = 2.22;
tiers.forEach((tr, i) => {
  const x = X + i * (cw6 + gap6);
  s.addShape(pres.ShapeType.roundRect, { x: x, y: y, w: cw6, h: tierH, rectRadius: 0.07,
    fill: { color: tr.c }, line: { color: tr.c, width: 0 }, objectName: "tier" + i });
  s.addText(tr.t, { x: x + 0.10, y: y + 0.16, w: cw6 - 0.20, h: 0.52, fontSize: 13.5, bold: true,
    color: tr.tc, isTextBox: true, margin: 0, align: "center", lineSpacingMultiple: 1.1,
    objectName: "tiert" + i });
  s.addText(tr.d, { x: x + 0.08, y: y + 0.72, w: cw6 - 0.16, h: 1.35, fontSize: 11, color: MUTED,
    isTextBox: true, margin: 0, align: "center", lineSpacingMultiple: 1.22, objectName: "tierd" + i });
  if (i < tiers.length - 1) {
    s.addText(">", { x: x + cw6, y: y + tierH / 2 - 0.22, w: gap6, h: 0.44, fontSize: 15, bold: true,
      color: RULE, isTextBox: true, margin: 0, align: "center", objectName: "arrow" + i });
  }
});
const fnTop5 = footnote(s, ["Deployment: nine services under Docker Compose on one workstation; the Spark cluster runs one master and two workers."]);
cardAt(s, { fill: GREY, head: "Why three tiers rather than one",
  body: "The dominant cost in this pipeline is repeated work. With a single tier, every schema change forces a fresh pull from OpenSky, which is rate-limited and intermittently unavailable, and every metric change forces a fresh cleaning pass, the most CPU-intensive stage. Separate tiers decouple both." },
  X, y + tierH + 0.24, W, fnTop5 - (y + tierH + 0.24) - 0.18);

/* 6 ------------------------------------------------------- data vault 2.0 */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Silver tier modelled as a Data Vault 2.0", { placeholder: "title" });
y = lead(s, "Hubs hold identity, links hold relationships, satellites hold attributes and are insertion-only.");

const TBLW = 6.30;
s.addTable(tableRows([
  ["Category", "Table", "Business key", "Rows"],
  ["Hub", "Aircraft", "airframe address", "226,661"],
  ["Hub", "Airport", "ICAO code", "18,595"],
  ["Hub", "Flight", "callsign, date", "98,426,017"],
  ["Link", "Flight to Aircraft", "flight, aircraft", "100,315,533"],
  ["Link", "Flight to Route", "flight, origin, dest.", "11,854,754"],
  ["Satellite", "Aircraft identity", "aircraft, typecode", "169,212"],
  ["Satellite", "Flight summary", "flight, first, last seen", "10,097,653"],
  ["Satellite", "State observation", "aircraft, time, position", "4,374,016"],
], { alignFn: (j) => (j === 3 ? "right" : "left") }),
  { x: X, y: y, w: TBLW, colW: [0.88, 1.72, 2.15, 1.55], rowH: 0.30,
    border: { type: "solid", color: "E3E7EC", pt: 1 }, margin: [2, 7, 2, 7],
    objectName: "vault-table" });

const RX = X + TBLW + 0.3, RW = W - TBLW - 0.3;
const fnTop6 = footnote(s, ["Keys: the flight hub is keyed on callsign and date, not airframe, because one airframe flies several distinct flights in a day. Hash keys replace natural keys in all joins."]);
fitColumn(s, [
  { head: "Multi-source reconciliation",
    body: "Three sources observe the same aircraft. A hub row is created once per key, so later observations attach to it instead of duplicating." },
  { head: "Additive schema evolution",
    body: "New attributes arrive as new satellites on an existing hub, so no existing table is altered and a schema change cannot break a stage." },
  { head: "Per-source lineage",
    body: "Every row records which ingest path produced it and when. Without it, no tier could be replayed in isolation." },
], RX, y, RW, fnTop6 - 0.18);
s.addNotes("Four lineage columns on every row: hash of the business key, load timestamp, record-source tag, partition month. Satellites are never updated in place, because an update destroys the history the load timestamp exists to record.");

/* 7 ----------------------------------------------- partitioning decision */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Monthly partitions made scaling measurable", { placeholder: "title" });
y = lead(s, "Every time-sensitive table is partitioned by calendar month. A watermark registry records the last month written per tier and per table, and advances inside the same transaction as the write it describes.");
y = cardRow(s, y, [
  { fill: WARM, headColor: ORANGE, head: "Before: full rebuild",
    body: "Every tier below bronze rebuilt completely on each run, so N months of data cost the sum of 1 through N months of work. Scaling efficiency cannot be measured on such a pipeline: wall time against volume measures rework. A scale-out attempt also exhausted the disk at roughly 20 GB of transient files." },
  { fill: PALE, head: "After: incremental refresh",
    body: "Adding one month processes one month. Partitions are written to a temporary path, flushed, then renamed into place, so a concurrent reader sees the complete old partition or the complete new one. A completion marker lets an interrupted run resume at a partition boundary." },
]);
statRow(s, y, [
  { value: "131 s", label: "to commit the second added month, from partition-commit markers" },
  { value: "148 s", label: "to commit the third added month: the same order, not growing" },
  { value: "linear", color: GREEN, label: "cost in data volume with a fixed setup term, in place of quadratic" },
], { size: 32 });
footnote(s, ["Setup: 1031 s warehouse build for three months of flight records. Month one additionally carries a one-time global build of the aircraft hub, identity satellite and global flight-to-aircraft link, occupying eleven minutes of that run."]);

/* 8 ------------------------------------------------------------- kafka */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Streaming ingest through Kafka", { placeholder: "title" });
y = lead(s, "Kafka buffers the live feed between the OpenSky REST API and the bronze tier. It was chosen over a task-queue broker, which optimises for acknowledgement rather than replayable streams, and over writing straight to storage.");
y = cardRow(s, y, [
  { head: "Topics and partitioning",
    body: "The raw topic has three partitions, matching the three consumer workers. The dead-letter topic has one, since it is written rarely and keeps global ordering for failure analysis." },
  { head: "Key is the airframe identifier",
    body: "All observations of one aircraft land on the same partition and are consumed in order. Without this, two consumers could write one aircraft out of order, corrupting the load-timestamp ordering that deduplication depends on." },
  { head: "At-least-once, deliberately",
    body: "Offsets commit only after a batch reaches storage. A crash replays the uncommitted window and silver absorbs the duplicates. Exactly-once was rejected: it solves a problem the data model already solves." },
]);
cardRow(s, y, [
  { fill: GREY, head: "All seventeen source fields published unmodified",
    body: "Projecting at ingest makes the raw record unrecoverable if a later stage needs a dropped field. Projection happens downstream, in the cleaning stage, never in the producer." },
  { fill: GREY, head: "Instrumented at the producer",
    body: "OAuth2 credentials refresh thirty seconds before expiry so a long-running producer never stalls mid-poll. Three metrics are scraped: records published, records dead-lettered by reason, and publish latency." },
]);
footnote(s, ["Elsewhere in the stack: PostgreSQL enforces the watermark format as a storage constraint, Spark carries the only table whose volume exceeds single-host memory, and Great Expectations blocks promotion on a failed suite."]);

/* 9 ------------------------------------------------------ three methods */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Three reconstruction methods spanning a cost range", { placeholder: "title" });
y = lead(s, "Three rather than two, because a cost-accuracy curve needs enough points to show its shape, and the three correspond to distinct families in the aviation literature.");
y = cardRow(s, y, [
  { fill: "F0F2F5", head: "METHOD A\nTime-gap baseline",
    body: "Observations group into one flight until the gap exceeds 1800 seconds, following published OpenSky practice for commercial operations. It evaluates no kinematic features, which makes it a clean reference point." },
  { fill: PALE, head: "METHOD B\nKinematic segmentation",
    body: "A rule over five features between consecutive observations: speed change, heading change, altitude rate, turn rate, callsign change. A break is declared when any exceeds a limit physically implausible for the aircraft class." },
  { fill: "FBEDE6", headColor: ORANGE, head: "METHOD C\nKinematic with Kalman smoothing",
    body: "The same segmenter with the position-jump break disabled, then a six-state constant-velocity Kalman filter per flight in a local East-North-Up frame, with a chi-squared innovation gate on the residual." },
]);
cardAt(s, { fill: NAVY, headColor: WHITE, bodyColor: "C7D2E0",
  head: "The design decision at the centre of Method C",
  body: "The jump rule and the filter's innovation gate treat the same situation, an observation whose implied motion is impossible, and resolve it differently: the rule starts a new flight, the gate down-weights a corrupt measurement. Only one can act on a given row. Method C disables the rule and lets the gate decide. The results say which is right." },
  X, y, W);
s.addNotes("Constant velocity was chosen over constant-acceleration and coordinated-turn models: higher-order models need more observations per segment to converge, and the research question is what a smoother adds, which the simplest filter isolates.");

/* 10 ----------------------------------------------------- noise + design */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Noise ladder and experiment design", { placeholder: "title" });
y = lead(s, "Real ADS-B carries no ground truth, so absolute position error cannot be measured against it. Four cumulative profiles degrade synthetic trajectories whose truth is known.");
y = cardRow(s, y, [
  { fill: "F0F2F5", head: "N0   Clean",
    body: "No modification. Establishes the correctness floor: a method that misses near-zero error here is broken, and the matrix must detect that." },
  { fill: "EDF1F6", head: "N1   Gaussian jitter",
    body: "Position and altitude jitter, approximating GPS multipath, receiver clock skew and atmospheric refraction." },
  { fill: "F5EDE8", headColor: ORANGE, head: "N2   Plus 10% message loss",
    body: "Receiver loss-of-signal. Stresses continuity and coverage: a gap-aware method must interpolate, not declare a false break." },
  { fill: "FBEDE6", headColor: ORANGE, head: "N3   Plus 10% reordering",
    body: "Two receivers forwarding one observation at different times. Tests whether event-time ordering survives scrambled arrival order." },
]);
cardRow(s, y, [
  { fill: PALE, head: "Input 1: synthetic, known truth",
    body: "40 flights and 10 injected deviations per cell. Answers the accuracy half of the question, since absolute position error and exact flight counts are measurable." },
  { fill: PALE, head: "Input 2: real OpenSky, 4,361,653 rows per cell",
    body: "No truth, but operational volume. Answers the cost half, and stops cost figures from a small truth-bearing sample being presented as scalability evidence." },
]);
footnote(s, ["Design: 3 methods by 4 profiles is 12 cells, run against both inputs, seed fixed at 42. Re-execution is idempotent under a uniqueness constraint, so every figure traces to one execution rather than the best of several."]);

/* 11 ---------------------------------------------- RESULTS section divider */
pres.addSection({ title: "Results" });
s = pres.addSlide({ masterName: "SECTION", sectionTitle: "Results" });
s.addText("EVALUATION", { placeholder: "eyebrow" });
s.addText("What the twelve-cell matrix measured", { placeholder: "title" });
s.addText("Accuracy on synthetic input with known truth, cost on 4.36 million real observations, and the answer to the research question.",
  { placeholder: "body" });

/* 12 ----------------------------------------------- accuracy vs cost plot */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("Accuracy against computational cost", { placeholder: "title" });
const fnTop12 = footnote(s, [
  "Setup: synthetic input, 14,400 observations per cell, truth 40 flights; wall time is per cell on the deployment host; the shaded band marks RMSE at or below 0.001 m.",
  "Result: time-gap and kinematic coincide in error at every profile and separate only in cost. The smoothed method costs an order of magnitude more and, except at N2, lands below the measurable error floor.",
]);
const imgH = fnTop12 - TOP - 0.22, imgW = imgH * (6.4 / 4.0);
s.addImage({ path: path.join(FIG, "fig1_accuracy_cost.png"),
  x: (13.3333 - imgW) / 2, y: TOP, w: imgW, h: imgH, objectName: "fig1" });

/* 13 ---------------------------------------------------- over-fragmentation */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("Kinematic segmentation over-fragments flights", { placeholder: "title" });
const fnTop13 = footnote(s, ["Result: the Kalman-smoothed method holds a stable 18,458 to 18,913 flights across all four profiles, close to the baseline's 14,948."]);
s.addChart([{
  type: pres.ChartType.bar,
  data: [
    { name: "Time-gap", labels: ["N0", "N1", "N2", "N3"], values: [14949, 14948, 14946, 14946] },
    { name: "Kinematic", labels: ["N0", "N1", "N2", "N3"], values: [102169, 108097, 134957, 163241] },
    { name: "Kinematic + Kalman", labels: ["N0", "N1", "N2", "N3"], values: [18913, 18814, 18679, 18458] },
  ],
}], Object.assign({}, chartBase, {
  x: X, y: TOP, w: 6.6, h: fnTop13 - TOP - 0.2,
  barDir: "col", barGrouping: "clustered", barGapWidthPct: 60,
  chartColors: [NAVY, ORANGE, GREEN],
  showTitle: true, title: "Flights reconstructed from 4.36 M real observations",
  showLegend: true, legendPos: "b",
  valAxisTitle: "Flights", showValAxisTitle: true,
  objectName: "chart-frag",
}));
fitColumn(s, [
  { fill: WARM, headColor: ORANGE, head: "Worse on real data than on synthetic",
    body: "On synthetic input it finds the true 40 flights when clean, then 68, 175 and 285 as noise rises. On real data it fragments even at N0: 102,169 against the baseline's 14,949." },
  { fill: GREY, head: "Why",
    body: "Natural receiver coverage gaps present to the rule exactly as injected noise does. The rule behaves correctly; the input is the problem." },
  { fill: GREY, head: "It costs the deviation metric too",
    body: "Deviation detection collapses to 2 flags against 10 injected events at N2: the fragments are too short to contain one." },
], X + 6.9, TOP, W - 6.9, fnTop13 - 0.18);

/* 14 ------------------------------------------------------- cost at scale */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("Cost and throughput at operational volume", { placeholder: "title" });
const fnTop14 = footnote(s, ["Setup: 4,361,653 real OpenSky state vectors per cell on the deployment host. Real input carries no ground truth, so this slide reports cost only."]);
s.addChart([{
  type: pres.ChartType.bar,
  data: [
    { name: "Time-gap", labels: ["N0", "N1", "N2", "N3"], values: [60.0, 59.4, 54.4, 56.1] },
    { name: "Kinematic", labels: ["N0", "N1", "N2", "N3"], values: [201.3, 215.8, 257.1, 312.5] },
    { name: "Kinematic + Kalman", labels: ["N0", "N1", "N2", "N3"], values: [353.7, 351.7, 335.1, 323.3] },
  ],
}], Object.assign({}, chartBase, {
  x: X, y: TOP, w: 6.6, h: fnTop14 - TOP - 0.2,
  barDir: "col", barGrouping: "clustered", barGapWidthPct: 60,
  chartColors: [NAVY, ORANGE, GREEN],
  showTitle: true, title: "Wall time per 4,361,653 real observations",
  showLegend: true, legendPos: "b",
  valAxisTitle: "Seconds", showValAxisTitle: true,
  objectName: "chart-cost",
}));
s.addTable(tableRows([
  ["Method", "Throughput (rec/s)", "vs baseline"],
  ["Time-gap", "72,732 to 80,247", "1.0x"],
  ["Kinematic", "13,958 to 21,670", "3.4x to 5.6x"],
  ["Kinematic + Kalman", "12,331 to 13,490", "5.8x to 6.2x"],
], { alignFn: (j) => (j === 0 ? "left" : "right") }),
  { x: X + 6.9, y: TOP, w: W - 6.9, colW: [2.07, 1.70, 1.26], rowH: 0.34,
    border: { type: "solid", color: "E3E7EC", pt: 1 }, margin: [2, 7, 2, 7],
    objectName: "tput-table" });
fitColumn(s, [
  { head: "The two methods scale differently in noise",
    body: "Baseline throughput is flat. Kinematic throughput falls from 21,670 to 13,958 as noise rises, because its cost is driven by the number of segments it creates and that number grows." },
  { head: "The smoother is flat to slightly rising",
    body: "12,331 to 13,490 records per second, because its cost is dominated by filtering observations and heavier profiles drop observations." },
], X + 6.9, TOP + 1.55, W - 6.9, fnTop14 - 0.18);

/* 15 ------------------------------------------------------ accuracy matrix */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("The twelve-cell accuracy matrix", { placeholder: "title" });
const fnTop15 = footnote(s, ["Truth: 40 flights and 10 injected deviations per cell.",
  "Caveat: the synthetic generator uses the same constant-velocity model the filter assumes, so sub-millimetre recovery is an upper bound on what the filter achieves against real manoeuvring aircraft."]);
const MTW = 6.90;
s.addTable(tableRows([
  ["Method", "Noise", "Wall (s)", "RMSE (m)", "Flights", "Deviations"],
  ["Time-gap", "N0", "0.154", "0.0", "40", "10"],
  ["Time-gap", "N1", "0.165", "4,901.5", "40", "10"],
  ["Time-gap", "N2", "0.158", "13,007.9", "40", "10"],
  ["Time-gap", "N3", "0.161", "16,869.3", "40", "10"],
  ["Kinematic", "N0", "0.155", "0.0", "40", "10"],
  ["Kinematic", "N1", "0.187", "4,901.5", "68", "8"],
  ["Kinematic", "N2", "0.337", "13,007.9", "175", "2"],
  ["Kinematic", "N3", "0.558", "16,869.3", "285", "3"],
  ["Kinematic + Kalman", "N0", "1.416", "< 0.001", "40", "10"],
  ["Kinematic + Kalman", "N1", "1.317", "< 0.001", "40", "10"],
  ["Kinematic + Kalman", "N2", "1.268", "1,445,595.8", "40", "11"],
  ["Kinematic + Kalman", "N3", "1.196", "< 0.001", "40", "10"],
], {
  size: 12.5,
  alignFn: (j) => (j < 2 ? "left" : "right"),
  colorFn: (i, j, c) => {
    if (j === 4 && c !== "40") return ORANGE;
    if (j === 3 && c === "< 0.001") return GREEN;
    if (j === 3 && c === "1,445,595.8") return RED;
    return BODY;
  },
}), { x: X, y: TOP, w: MTW, colW: [1.70, 0.62, 0.88, 1.40, 1.05, 1.25], rowH: 0.295,
  border: { type: "solid", color: "E3E7EC", pt: 1 }, margin: [2, 7, 2, 7],
  objectName: "matrix-table" });

const MX = X + MTW + 0.3, MW = W - MTW - 0.3;
fitColumn(s, [
  { head: "Segmentation and estimation are separable",
    body: "Time-gap and kinematic report identical RMSE at every profile. Neither imputes a position, so accuracy comes from estimation, not better boundaries." },
  { fill: MINT, headColor: GREEN, head: "The gate beats the rule",
    body: "Method C holds the flight count at truth under every profile, where the jump rule fragmented into 285 at N3." },
  { fill: WARM, headColor: RED, head: "One cell needs explaining",
    body: "The N2 smoothed cell drifts because its gate, tuned on N1 residuals, rejects valid N2 measurements. One threshold value." },
], MX, TOP, MW, fnTop15 - 0.10, 0.15);

/* 16 ------------------------------------------------------------ delivery */
pres.addSection({ title: "Summary" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
s.addText("What was delivered against the proposal", { placeholder: "title" });
y = lead(s, "All seven design goals and all six performance targets were delivered and measured on the deployed system.");
y = statRow(s, y, [
  { value: "14.1 GB", label: "raw ingested, 121.6 M records across 48 months of archives" },
  { value: "244.5 M", label: "warehouse rows written: 225.5 M silver, 19.0 M gold" },
  { value: "12 / 12", label: "experiment cells populated, on each of the two inputs" },
  { value: "6x", label: "cost of the smoother over the baseline at operational volume" },
], { size: 28, fill: PALE, color: NAVY });
statRow(s, y, [
  { value: "92 / 92", label: "automated tests passing, in 8.25 seconds" },
  { value: "25 / 25", label: "data-quality expectations passing across five suites" },
  { value: "7 / 7", label: "rollback scenarios passing, downstream-first ordering verified" },
  { value: "17x", label: "speedup from vectorising cross-track distance, the largest win" },
], { size: 28, fill: GREY, color: BODY });
footnote(s, ["Verification: three cross-tier identity checks hold exactly, confirming no rows lost or duplicated between silver and gold. Determinism was checked by byte-level hash equality on every output partition across two runs."]);

/* 17 ------------------------------------------------------------- answer */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
s.addText("Answer to the research question", { placeholder: "title" });
s.addShape(pres.ShapeType.roundRect, { x: X, y: TOP, w: W, h: 0.88, rectRadius: 0.06,
  fill: { color: PALE }, line: { color: PALE, width: 0 }, objectName: "q-bg" });
s.addText("How much accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?",
  { x: X + 0.3, y: TOP + 0.14, w: W - 0.6, h: 0.60, fontSize: 16.5, italic: true, color: NAVY,
    isTextBox: true, margin: 0, lineSpacingMultiple: 1.18, objectName: "q" });
y = cardRow(s, TOP + 1.04, [
  { fill: WARM, headColor: ORANGE, head: "Kinematic segmentation alone gains nothing",
    body: "It produces identical position error to the baseline at every noise level, because neither estimates positions. Under noise it is worse than the baseline at the task it was meant to improve, splitting 40 true flights into 285, and the same appears on real data with no injected noise." },
  { fill: MINT, headColor: GREEN, head: "The gain comes from the estimator",
    body: "The Kalman filter reduces position error from kilometres to below a millimetre, and by handling implausible observations as corrupt measurements rather than as flight boundaries it also holds the flight count at truth where the kinematic rule alone fragmented it sevenfold." },
  { fill: PALE, head: "The cost is about six times the baseline",
    body: "Measured on 4.36 million real observations: 5.8 to 6.2 times the baseline wall time, and 7.4 to 9.2 times on the smaller synthetic input. Within each profile the frontier holds the baseline and the smoother; kinematic segmentation is dominated everywhere." },
]);
cardAt(s, { fill: NAVY, headColor: WHITE, bodyColor: "C7D2E0", head: "The practical reading",
  body: "The trade is worthwhile when the trajectory itself is the object of analysis, and not worthwhile when only flight boundaries are required: those are well served by the baseline at a sixth of the cost. This is a sharper answer than the proposal anticipated, because it attributes the gain to estimation rather than to kinematic reasoning in general." },
  X, y, W);

/* 18 ------------------------------------------------------- future work */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
s.addText("Where this goes next", { placeholder: "title" });
y = lead(s, "Three extensions, in order of how little they change the delivered system.");
const nexts = [
  { n: "1", t: "Promote the remaining builders to Spark",
    b: "The monthly partition contract and watermark semantics are engine-independent, so extending Spark execution from the state-observation satellite to the flight-record builders is a substitution of engine, not a redesign. It moves the last single-host processing onto the cluster." },
  { n: "2", t: "Run the full 48-month archive",
    b: "At the measured marginal cost of 131 to 148 seconds per month, materialising the whole archive gives the accuracy-cost frontier its full historical depth, including seasonal variation in traffic density." },
  { n: "3", t: "Adopt a managed table format",
    b: "Apache Iceberg or Delta Lake would add snapshot isolation and time-travel queries in place of the custom watermark implementation. The current implementation mirrors their semantics closely enough that this is a substitution rather than a rewrite." },
];
nexts.forEach((nx) => {
  const tw = W - 0.95;
  const th = textH(nx.t, 16.5, tw), bh = textH(nx.b, 14, tw, 1.2);
  s.addShape(pres.ShapeType.ellipse, { x: X, y: y + 0.04, w: 0.58, h: 0.58,
    fill: { color: NAVY }, line: { color: NAVY, width: 0 }, objectName: nm("nc") });
  s.addText(nx.n, { x: X, y: y + 0.04, w: 0.58, h: 0.58, fontSize: 19, bold: true,
    color: WHITE, align: "center", valign: "middle", isTextBox: true, margin: 0,
    objectName: nm("nn") });
  s.addText(nx.t, { x: X + 0.95, y: y, w: tw, h: th + 0.04, fontSize: 16.5, bold: true,
    color: NAVY, isTextBox: true, margin: 0, objectName: nm("nt") });
  s.addText(nx.b, { x: X + 0.95, y: y + th + 0.09, w: tw, h: bh + 0.04, fontSize: 14, color: BODY,
    isTextBox: true, margin: 0, lineSpacingMultiple: 1.2, objectName: nm("nb") });
  y += th + bh + 0.46;
});
footnote(s, ["Accompanying the deck: source code, 92 automated tests, eight Grafana dashboards, the append-only decision log and the reproducibility fixtures."]);

/* --------------------------------------------------------------- write */
const OUT = "B:/IISC/DE/Proj/etl4/docs/ETL4_Project_Presentation.pptx";
(async () => {
  await pres.writeFile({ fileName: OUT });
  const { applyTheme } = require("C:/Users/avina/.claude/skills/synced/e174a0e3-be4a-4e49-9d72-175513a4de71_3b498e84-05af-4202-9b3a-f2a4c9d06330/pptx/scripts/apply_theme.js");
  await applyTheme(OUT, THEME);
  console.log("wrote " + OUT);
})();
