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
    // Only the title is a placeholder. A layout placeholder no slide fills is
    // copied onto every slide as an empty "Click to add text" prompt box.
    { placeholder: {
        options: { name: "title", type: "title", x: X, y: 0.38, w: W, h: 0.62,
                   fontSize: 30, bold: true, color: NAVY, valign: "middle", margin: 0 },
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
// Pass `bottom` for a row that must clear the footnote: it throws at build time
// instead of silently overlapping, which the geometry audit would only catch later.
function cardRow(slide, y, cards, opts) {
  const o = opts || {};
  const gap = o.gap === undefined ? 0.22 : o.gap;
  const total = o.w || W;
  const cw = (total - gap * (cards.length - 1)) / cards.length;
  const h = o.h || Math.max(...cards.map((c) => cardHeight(c, cw)));
  if (o.bottom !== undefined && y + h > o.bottom + 0.001) {
    throw new Error(`cardRow overflow: row ends at ${(y + h).toFixed(2)}in, limit ` +
                    `${o.bottom.toFixed(2)}in [first head: ${cards[0].head.split("\n")[0]}]`);
  }
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
s.addNotes("This is a course project for DA 231o. The deliverable has two parts: a lakehouse pipeline, and a measured comparison of three reconstruction methods.");

/* 2 ------------------------------------------- problem: not trajectories */
pres.addSection({ title: "Problem" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
s.addText("ADS-B gives us position reports, not flights", { placeholder: "title" });
let y = lead(s, "Aircraft broadcast a position about once each second. OpenSky collects these broadcasts from several thousand receivers. The result is one mixed stream, and it does not mark flight boundaries.");
cardRow(s, y, [
  { head: "1.  Order the stream",
    body: "Group the reports by aircraft. Sort them by observation time. Reports arrive out of order, so arrival time is not the event time." },
  { head: "2.  Find the boundaries",
    body: "Find where one flight ends and the next one starts. One airframe flies several flights in a day. The raw stream does not separate them." },
  { head: "3.  Repair the path",
    body: "Make the trajectory continuous and physically possible. Some points are absent. Receivers that overlap duplicate other points. Some values are corrupt." },
]);
footnote(s, [
  "Source: OpenSky Network monthly flight-record archives 2019 to 2022, daily state-vector archives, and the live REST feed.",
  "Scope: convert the stream into trajectories at scale, implement three methods for the boundary problem, and measure the accuracy and the cost of each one.",
]);
s.addNotes("The raw stream is not the deliverable. Every stage after it converts the stream into flights.");

/* 3 --------------------------------------------- motivation + question */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
s.addText("Why the standard time-gap rule is not enough", { placeholder: "title" });
y = lead(s, "The usual rule splits a flight when two consecutive observations are more than 30 minutes apart. The rule reads elapsed time only. It ignores the motion that the two observations imply.");
y = cardRow(s, y, [
  { fill: WARM, headColor: ORANGE, head: "A long gap is not always a new flight",
    body: "Two aircraft give the same 40-minute gap. The first one parks between legs. The second one cruises, but loses receiver coverage. The rule splits both. One split is wrong." },
  { fill: WARM, headColor: ORANGE, head: "A short gap is not always continuous",
    body: "Two reports three seconds apart imply a 200 km jump. That motion is impossible, so one report is corrupt. The rule accepts both, because the time gap is small." },
]);
s.addShape(pres.ShapeType.roundRect, { x: X, y: y + 0.1, w: W, h: 1.25, rectRadius: 0.06,
  fill: { color: NAVY }, line: { color: NAVY, width: 0 }, objectName: "rq-bg" });
s.addText([
  { text: "RESEARCH QUESTION", options: { fontSize: 11.5, bold: true, color: RULE, charSpacing: 2, breakLine: true } },
  { text: "How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?",
    options: { fontSize: 16.5, color: WHITE, italic: true } },
], { x: X + 0.3, y: y + 0.24, w: W - 0.6, h: 0.98, isTextBox: true, margin: 0,
     lineSpacingMultiple: 1.18, objectName: "rq" });
footnote(s, ["Resolution: kinematics correct both cases. Speed, heading change, altitude rate and turn rate show if the aircraft class permits the implied motion."]);

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

const fnTop4 = footnote(s, ["Constraints: a fixed seed makes every stage reproducible. An append-only decision log records each schema change and each parameter change."]);
const c4 = { fill: PALE, head: "Target 5 drove the architecture",
  body: "Scaling efficiency asks one question. Does cost grow with the new data, or with the total data? A pipeline that rebuilds all data on each run costs the square of the dataset. No per-stage optimisation corrects this. The next section follows from this target." };
cardAt(s, c4, colR, fnTop4 - 0.2 - cardHeight(c4, colW), colW);

/* 5 -------------------------------------------------------- architecture */
pres.addSection({ title: "Approach" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Pipeline architecture", { placeholder: "title" });
y = lead(s, "Seven stages in a linear flow. Each tier writes its output to disk. A downstream tier therefore rebuilds without a repeat of the work above it.");

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
const fnTop5 = footnote(s, ["Deployment: Docker Compose runs nine services on one workstation. The Spark cluster has one master and two workers."]);
cardAt(s, { fill: GREY, head: "Why three tiers rather than one",
  body: "Repeated work is the largest cost in this pipeline. A single tier has two penalties. Each schema change forces a new pull from OpenSky, and OpenSky limits the request rate. Each metric change forces a new cleaning pass, the most expensive stage. Separate tiers remove both penalties." },
  X, y + tierH + 0.24, W, fnTop5 - (y + tierH + 0.24) - 0.18);

/* 6 ------------------------------------------------------- data vault 2.0 */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("The silver tier uses a Data Vault 2.0 model", { placeholder: "title" });
y = lead(s, "Hubs hold identity. Links hold relationships. Satellites hold attributes, and take new rows only.");

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
const fnTop6 = footnote(s, ["Keys: the flight hub uses callsign and date, not airframe, because one airframe flies several flights in a day. Hash keys replace natural keys in every join."]);
fitColumn(s, [
  { head: "Multi-source reconciliation",
    body: "Three sources observe the same aircraft. The builder creates one hub row for each key. A later observation attaches to that row." },
  { head: "Additive schema evolution",
    body: "A new attribute arrives as a new satellite on an existing hub. No existing table changes, so a schema change does not break a stage." },
  { head: "Per-source lineage",
    body: "Every row records its ingest path and its load time. Without this record, no tier replays on its own." },
], RX, y, RW, fnTop6 - 0.18);
s.addNotes("Every row has four lineage columns: business-key hash, load timestamp, record-source tag, and partition month. The builder never updates a satellite in place. An update destroys the history that the load timestamp records.");

/* 7 ----------------------------------------------- partitioning decision */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Monthly partitions made scaling measurable", { placeholder: "title" });
y = lead(s, "Each time-sensitive table uses the calendar month as its partition. A watermark registry records the last month for each tier and each table. The registry advances in the same transaction as the write.");
y = cardRow(s, y, [
  { fill: WARM, headColor: ORANGE, head: "Before: full rebuild",
    body: "Each run rebuilt every tier below bronze. N months of data therefore cost the sum of 1 to N months of work. This pipeline hides scaling efficiency, because wall time against volume shows rework only. A scale-out attempt also filled the disk with about 20 GB of temporary files." },
  { fill: PALE, head: "After: incremental refresh",
    body: "One added month costs one month of work. The writer puts a partition in a temporary path, flushes it, then renames it into place. A parallel reader therefore sees the complete old partition or the complete new one. A completion marker restarts a stopped run at a partition boundary." },
]);
statRow(s, y, [
  { value: "131 s", label: "to commit the second added month, from the partition-commit markers" },
  { value: "148 s", label: "to commit the third added month: the same order, with no growth" },
  { value: "linear", color: GREEN, label: "cost in data volume with a fixed setup term, in place of quadratic" },
], { size: 32 });
footnote(s, ["Setup: the warehouse build took 1031 s for three months of flight records. Month one also carries one global build of the aircraft hub, the identity satellite and the global flight-to-aircraft link. That global build took 11 minutes of the run."]);

/* 8 ------------------------------------------------------------- kafka */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Streaming ingest through Kafka", { placeholder: "title" });
const fnTop8 = footnote(s, ["Dead-letter policy: the producer diverts a record with a malformed array shape, and a record that fails to serialise, and it keeps the failure reason. The producer retries a failed poll on the next interval, so a short outage costs one window, not the run."]);
y = lead(s, "Kafka buffers the live feed between the OpenSky REST API and the bronze tier. A task-queue broker optimises acknowledgement, not replayable streams.");
y = cardRow(s, y, [
  { head: "Topics and partitions",
    body: "The raw topic has three partitions, one for each consumer worker. The dead-letter topic has one. It takes few writes, and one partition keeps global order for failure analysis." },
  { head: "The key is the airframe identifier",
    body: "Every observation of one aircraft goes to the same partition, and one consumer reads them in order. Without this key, two consumers corrupt the load-timestamp order that deduplication needs." },
  { head: "At-least-once, by decision",
    body: "The consumer commits offsets after a batch reaches storage. A crash replays the open window, and silver absorbs the duplicates. Exactly-once delivery solves a problem that the data model solves already." },
]);
cardRow(s, y, [
  { fill: GREY, head: "The producer publishes all 17 source fields",
    body: "A projection at ingest loses the raw record. A later stage then has no way to read a field that the projection dropped. The clean stage does the projection instead." },
  { fill: GREY, head: "The producer reports three metrics",
    body: "The OAuth2 token refreshes 30 seconds before it expires, so a long run never stops in mid-poll. Prometheus scrapes three metrics: records published, records sent to the dead-letter topic by reason, and publish latency." },
], { bottom: fnTop8 - 0.18 });
s.addNotes("A direct write from the API to storage was the other option. It couples the poll rate to the write rate, and it loses data whenever the write path stalls. Kafka removes both faults.");

/* 9 --------------------------------------------------- platform stack */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Big data platforms, and the reason for each one", { placeholder: "title" });
y = lead(s, "What each platform does in the pipeline, and the property that decided it.");
s.addTable(tableRows([
  ["Platform", "Role in the pipeline", "Why this one"],
  ["Apache Spark", "Distributed build of the state-observation satellite",
   "The only table whose source volume exceeds single-host memory"],
  ["PostgreSQL", "Telemetry, per-metric results, watermark registry, dashboard tables",
   "Constraints reject an invalid watermark at the storage boundary, not in caller code"],
  ["Prometheus + Grafana", "Scrapes Spark, host and producer metrics; eight dashboard panels",
   "The six performance targets map onto counters, histograms and gauges"],
  ["Apache Airflow", "Schedules the streaming path, branches live against batch, retries",
   "A dedicated script runs the batch path, because a script is simpler to time per stage"],
  ["Great Expectations", "25 expectations across 5 suites at the bronze and mart boundaries",
   "The suites are declarative, and one failure blocks promotion of that table"],
  ["Apache Parquet", "Columnar storage with a dataset abstraction",
   "Partition predicates push into file listing; an explicit type keeps a hex id a string"],
  ["Docker Compose", "Runs the nine-service stack on one host",
   "The target is one workstation, where a cluster control plane adds cost only"],
], { size: 11.5, headSize: 11.5 }),
  { x: X, y: y, w: W, colW: [1.70, 4.35, 5.88], rowH: 0.40,
    border: { type: "solid", color: "E3E7EC", pt: 1 }, margin: [2, 7, 2, 7],
    objectName: "platform-table" });
footnote(s, ["The previous slide covers Kafka. One projection function does the schema convergence, so the three source schemas meet the canonical schema in one place."]);
s.addNotes("Airflow orchestrates the streaming path. A dedicated script runs the batch path for the Section 3 measurements, because per-stage timing is simpler to instrument in a script.");

/* 10 ----------------------------------------------------- three methods */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Three reconstruction methods across a cost range", { placeholder: "title" });
y = lead(s, "The project implements three methods, not two. A cost-accuracy curve needs three points to show its shape. The three methods also match three families in the aviation literature.");
y = cardRow(s, y, [
  { fill: "F0F2F5", head: "METHOD A\nTime-gap baseline",
    body: "Observations group into one flight until the gap exceeds 1800 seconds. This threshold follows OpenSky practice for commercial operations. The method reads no kinematic feature, so it gives a clean reference point." },
  { fill: PALE, head: "METHOD B\nKinematic segmentation",
    body: "A rule reads five features between consecutive observations: speed change, heading change, altitude rate, turn rate, and callsign change. The rule declares a break when one feature exceeds the limit for the aircraft class." },
  { fill: "FBEDE6", headColor: ORANGE, head: "METHOD C\nKinematic with Kalman smoothing",
    body: "The same segmenter runs with the position-jump break off. A six-state constant-velocity Kalman filter then runs for each flight in a local East-North-Up frame. A chi-squared gate tests the residual." },
]);
cardAt(s, { fill: NAVY, headColor: WHITE, bodyColor: "C7D2E0",
  head: "The design decision at the centre of Method C",
  body: "The jump rule and the innovation gate treat the same condition: an observation with impossible implied motion. They differ in the result. The rule starts a new flight. The gate gives a corrupt measurement a lower weight. Only one of the two acts on a row, so Method C turns the rule off. The measurements show which choice is correct." },
  X, y, W);
s.addNotes("The project uses a constant-velocity model, not a constant-acceleration or coordinated-turn model. A higher-order model needs more observations in each segment. The research question asks what a smoother adds, and the simplest filter isolates that contribution.");

/* 11 ----------------------------------------------------- noise + design */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
s.addText("Noise ladder and experiment design", { placeholder: "title" });
const fnTop11 = footnote(s, [
  "Design: 3 methods by 4 profiles gives 12 cells. Each cell runs against both inputs, with seed 42. A uniqueness constraint makes a repeat run idempotent, so every figure traces to one execution.",
  "Six metrics per cell: segmentation precision, recall and F1 within a 5 s tolerance; boundary time error; trajectory continuity; coverage; position RMSE; deviation-detection precision, recall and F1.",
]);
y = lead(s, "Real ADS-B has no ground truth, so absolute position error needs a synthetic input. Four cumulative profiles degrade synthetic trajectories with known truth.");
y = cardRow(s, y, [
  { fill: "F0F2F5", head: "N0   Clean",
    body: "No modification. This profile sets the correctness floor. A correct method reaches near-zero error here." },
  { fill: "EDF1F6", head: "N1   Gaussian jitter",
    body: "Jitter on position and altitude. It approximates GPS multipath, receiver clock skew and refraction." },
  { fill: "F5EDE8", headColor: ORANGE, head: "N2   Plus 10% message loss",
    body: "Receiver loss of signal. This profile tests continuity and coverage. A gap-aware method declares no false break." },
  { fill: "FBEDE6", headColor: ORANGE, head: "N3   Plus 10% reordering",
    body: "Two receivers send one observation at different times. This profile tests event-time order." },
]);
cardRow(s, y, [
  { fill: PALE, head: "Input 1: synthetic, known truth",
    body: "40 flights and 10 injected deviations per cell. It answers the accuracy half, because position error and flight count are measurable." },
  { fill: PALE, head: "Input 2: real OpenSky, 4,361,653 rows per cell",
    body: "No truth, but operational volume. This input answers the cost half. It also keeps a small sample out of the role of scalability evidence." },
], { bottom: fnTop11 - 0.18 });

/* 12 ---------------------------------------------- RESULTS section divider */
pres.addSection({ title: "Results" });
s = pres.addSlide({ masterName: "SECTION", sectionTitle: "Results" });
s.addText("EVALUATION", { placeholder: "eyebrow" });
s.addText("What the twelve-cell matrix measured", { placeholder: "title" });
s.addText("Accuracy on synthetic input with known truth, cost on 4.36 million real observations, and the answer to the research question.",
  { placeholder: "body" });

/* 13 ----------------------------------------------- accuracy vs cost plot */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("Accuracy against computational cost", { placeholder: "title" });
const fnTop12 = footnote(s, [
  "Setup: synthetic input, 14,400 observations per cell, truth 40 flights. Wall time is per cell on the deployment host. The shaded band marks RMSE at or below 0.001 m.",
  "Result: time-gap and kinematic give the same error at every profile, and they differ in cost only. The smoothed method costs an order of magnitude more. At N0, N1 and N3 its error falls below the measurable floor.",
]);
const imgH = fnTop12 - TOP - 0.22, imgW = imgH * (6.4 / 4.0);
s.addImage({ path: path.join(FIG, "fig1_accuracy_cost.png"),
  x: (13.3333 - imgW) / 2, y: TOP, w: imgW, h: imgH, objectName: "fig1" });

/* 14 --------------------------------------------------- over-fragmentation */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("Kinematic segmentation over-fragments flights", { placeholder: "title" });
const fnTop13 = footnote(s, ["Result: the Kalman-smoothed method holds 18,458 to 18,913 flights across the four profiles. The baseline gives 14,948."]);
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
    body: "On clean synthetic input the rule finds the true 40 flights. It then gives 68, 175 and 285 as noise rises. On real data it fragments at N0 already: 102,169 flights against 14,949 for the baseline." },
  { fill: GREY, head: "Why",
    body: "A natural gap in receiver coverage looks the same to the rule as injected noise. The rule operates correctly. The input causes the result." },
  { fill: GREY, head: "It costs the deviation metric too",
    body: "At N2 the method flags 2 events against 10 injected events. Each fragment is too short to hold one deviation." },
], X + 6.9, TOP, W - 6.9, fnTop13 - 0.18);

/* 15 ------------------------------------------------------- cost at scale */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("Cost and throughput at operational volume", { placeholder: "title" });
const fnTop14 = footnote(s, ["Setup: each cell uses 4,361,653 real OpenSky state vectors on the deployment host. Real input has no ground truth, so this slide reports cost only."]);
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
    body: "Baseline throughput stays flat. Kinematic throughput falls from 21,670 to 13,958 as noise rises. The segment count drives its cost, and that count grows." },
  { head: "The smoother stays flat",
    body: "12,331 to 13,490 records each second. The filter step dominates its cost, and a heavier profile drops observations." },
], X + 6.9, TOP + 1.55, W - 6.9, fnTop14 - 0.18);

/* 16 ------------------------------------------------------ accuracy matrix */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("The twelve-cell accuracy matrix", { placeholder: "title" });
const fnTop15 = footnote(s, ["Truth: each cell has 40 flights and 10 injected deviations.",
  "Caveat: the synthetic generator uses the same constant-velocity model as the filter. Sub-millimetre recovery is therefore an upper bound for a real aircraft in a manoeuvre."]);
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
  { head: "Segmentation and estimation separate",
    body: "Time-gap and kinematic report the same RMSE at every profile. Neither estimates a position, so accuracy comes from estimation, not from boundaries." },
  { fill: MINT, headColor: GREEN, head: "The gate beats the rule",
    body: "Method C holds the flight count at truth in every profile. The jump rule gave 285 flights at N3." },
  { fill: WARM, headColor: RED, head: "One cell needs an explanation",
    body: "The N2 smoothed cell drifts. Its gate threshold comes from N1 residuals, and rejects valid N2 measurements. One value causes the fault." },
], MX, TOP, MW, fnTop15 - 0.10, 0.15);

/* 17 -------------------------------------------- six performance targets */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
s.addText("The six performance targets, instrumented and measured", { placeholder: "title" });
y = lead(s, "The project gave each target an instrument first, then took the measurement. Two targets read as a dashboard series, not as a single number.");
s.addTable(tableRows([
  ["Target", "Instrumentation", "Measured result"],
  ["Throughput", "Per-cell row counts and wall time; producer counter scraped continuously",
   "72,700 to 80,200 rec/s baseline;\n12,300 to 13,500 rec/s smoothed"],
  ["End-to-end time and streaming latency",
   "Every stage persists its elapsed time; publish-latency histogram, 11 buckets from 1 ms to 5 s",
   "1031 s warehouse build for 3 months,\n112.2 s downstream; latency percentiles on the streaming dashboard"],
  ["CPU and memory utilisation", "Host and container gauges scraped continuously",
   "Resource dashboard"],
  ["Shuffle and partitioning overhead", "Spark metrics endpoint reports shuffle bytes",
   "No shuffle on the single-host path, by construction"],
  ["Scaling efficiency", "Partition-commit timestamps, per month and per table",
   "131 s and 148 s for the second and third added month"],
  ["Robustness", "The N0 to N3 ladder: missing, duplicated and out-of-order records, on both inputs",
   "12 of 12 cells complete on each input"],
], { size: 11.5, headSize: 11.5 }),
  { x: X, y: y, w: W, colW: [2.30, 5.18, 4.45], rowH: 0.40,
    border: { type: "solid", color: "E3E7EC", pt: 1 }, margin: [2, 7, 2, 7],
    objectName: "targets-table" });
footnote(s, [
  "Downstream detail: of the 112.2 s, the marts take 70.5 s and data quality takes 27.4 s. The matrix takes 13.3 s, and the evaluation store 0.8 s.",
  "Scaling efficiency shaped the architecture. The partition-commit markers make the marginal cost of an added month a direct measurement, not an average over the whole run.",
]);
s.addNotes("Two rows point to a dashboard. Latency and resource use have instruments, and both appear as a series, but neither gives one headline number. The single-host path makes no shuffle by construction, not by optimisation.");

/* 18 ------------------------------------------------------------ delivery */
pres.addSection({ title: "Summary" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
s.addText("Delivery against the proposal", { placeholder: "title" });
y = lead(s, "The deployed system meets all seven design goals and all six performance targets.");
y = statRow(s, y, [
  { value: "14.1 GB", label: "raw data, 121.6 M records across 48 months of archives" },
  { value: "244.5 M", label: "warehouse rows written: 225.5 M silver, 19.0 M gold" },
  { value: "12 / 12", label: "experiment cells complete, on each of the two inputs" },
  { value: "6x", label: "cost of the smoother over the baseline at operational volume" },
], { size: 28, fill: PALE, color: NAVY });
statRow(s, y, [
  { value: "92 / 92", label: "automated tests pass, in 8.25 seconds" },
  { value: "25 / 25", label: "data-quality expectations pass, across five suites" },
  { value: "7 / 7", label: "rollback scenarios pass, in downstream-first order" },
  { value: "17x", label: "speedup from the vectorised cross-track distance, the largest gain" },
], { size: 28, fill: GREY, color: BODY });
footnote(s, ["Verification: three cross-tier identity checks hold exactly. Silver and gold therefore lose no row and duplicate no row. A byte-level hash of every output partition matched across two runs."]);

/* 19 ------------------------------------------------------------- answer */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
s.addText("Answer to the research question", { placeholder: "title" });
s.addShape(pres.ShapeType.roundRect, { x: X, y: TOP, w: W, h: 0.88, rectRadius: 0.06,
  fill: { color: PALE }, line: { color: PALE, width: 0 }, objectName: "q-bg" });
s.addText("How much accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?",
  { x: X + 0.3, y: TOP + 0.14, w: W - 0.6, h: 0.60, fontSize: 16.5, italic: true, color: NAVY,
    isTextBox: true, margin: 0, lineSpacingMultiple: 1.18, objectName: "q" });
y = cardRow(s, TOP + 1.04, [
  { fill: WARM, headColor: ORANGE, head: "Kinematic segmentation alone gains nothing",
    body: "The method gives the same position error as the baseline at every profile, because neither method estimates a position. Under noise it is worse at its own task: it splits 40 true flights into 285. On real data it fragments with no injected noise." },
  { fill: MINT, headColor: GREEN, head: "The gain comes from the estimator",
    body: "The Kalman filter cuts position error from kilometres to below a millimetre. It treats an implausible observation as a corrupt measurement, not as a flight boundary. It therefore holds the flight count at truth, where the rule alone fragmented it sevenfold." },
  { fill: PALE, head: "The cost is about six times the baseline",
    body: "On 4.36 million real observations the smoother costs 5.8 to 6.2 times the baseline wall time. On the smaller synthetic input it costs 7.4 to 9.2 times. The smoother joins the frontier at N1 and N3. At N0 and N2 the baseline holds the frontier alone, and the other two methods dominate kinematic segmentation everywhere." },
]);
cardAt(s, { fill: NAVY, headColor: WHITE, bodyColor: "C7D2E0", head: "The practical reading",
  body: "The cost is worthwhile when the trajectory itself is the object of analysis. It is not worthwhile when the work needs flight boundaries only, because the baseline gives those at a sixth of the cost. This answer is sharper than the proposal expected: the gain comes from estimation, not from kinematics in general." },
  X, y, W);

/* 20 ------------------------------------------------------- future work */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
s.addText("Where this goes next", { placeholder: "title" });
y = lead(s, "Three extensions, in order of how little each one changes the delivered system.");
const nexts = [
  { n: "1", t: "Move the remaining builders to Spark",
    b: "The monthly partition contract and the watermark semantics are engine-independent. A move of Spark execution from the state-observation satellite to the flight-record builders changes the engine only. It is not a redesign. It also puts the last single-host work on the cluster." },
  { n: "2", t: "Run the full 48-month archive",
    b: "The marginal cost is 131 to 148 seconds for each month. A run over the whole archive gives the accuracy-cost frontier its full historical depth. It also shows the seasonal change in traffic density." },
  { n: "3", t: "Adopt a managed table format",
    b: "Apache Iceberg or Delta Lake adds snapshot isolation and time-travel queries, and replaces the custom watermark code. The current code follows the same semantics, so this change is a substitution, not a rewrite." },
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
footnote(s, ["The deck has these attachments: source code, 92 automated tests, eight Grafana dashboards, the append-only decision log, and the reproducibility fixtures."]);

/* --------------------------------------------------------------- write */
const OUT = process.env.DECK_OUT || "B:/IISC/DE/Proj/etl4/docs/ETL4_Project_Presentation.pptx";
(async () => {
  await pres.writeFile({ fileName: OUT });
  const { applyTheme } = require("C:/Users/avina/.claude/skills/synced/e174a0e3-be4a-4e49-9d72-175513a4de71_3b498e84-05af-4202-9b3a-f2a4c9d06330/pptx/scripts/apply_theme.js");
  await applyTheme(OUT, THEME);
  console.log("wrote " + OUT);
})();
