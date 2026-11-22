/* ETL4 project presentation generator.
 *
 * Type scale and palette are fixed by the course design rules: Arial
 * throughout, 32 pt bold slide titles, 22 pt bold section headings, 18 pt bold
 * subheadings, 14 pt body, 11 pt footnotes, navy #002D56 on white, #333333
 * body text, #666666 secondary. Text is left-aligned everywhere, including
 * numeric table columns.
 *
 * Content is never boxed for decoration. A rectangle appears only where it
 * carries information: the architecture diagram and the table header band.
 *
 * Build:
 *   DECK_OUT=<path> APPLY_THEME_JS=<apply_theme.js> node build_presentation.js
 */
const pptxgen = require("pptxgenjs");
const path = require("path");

const FIG = path.join(__dirname, "figures");

const NAVY = "002D56", BODY = "333333", MUTED = "666666",
      RULE = "D4DAE2", ZEBRA = "F4F6F8", WHITE = "FFFFFF",
      // Monochrome navy ramp for chart series: no rainbow, readable on white.
      SER1 = "002D56", SER2 = "5A7490", SER3 = "9FB0C2";

const THEME = {
  name: "ETL4 ADS-B",
  headFontFace: "Arial",
  bodyFontFace: "Arial",
  colors: {
    dk1: BODY, lt1: WHITE,
    dk2: NAVY, lt2: ZEBRA,
    accent1: SER1, accent2: SER2, accent3: SER3,
    accent4: RULE, accent5: MUTED, accent6: NAVY,
    hlink: NAVY, folHlink: MUTED,
  },
};

/* ---------------------------------------------------------------- metrics */
const SW = 13.3333;
const X = 0.75, W = 11.83;        // left margin and content width
const TITLE_Y = 0.42, TITLE_H = 0.60, TITLE_PT = 32;
const HEAD_RULE_Y = 1.14;         // navy rule under the title
const TOP = 1.38;                 // first content baseline
const FOOT_RULE_Y = 6.98;         // footer rule on the layout
const BOT = 6.84;                 // nothing below this on a content slide

const PT_SECTION = 22, PT_SUB = 18, PT_BODY = 14, PT_NOTE = 11;

/* ------------------------------------------------- text measurement ----- */
// Mirrors audit.py so the generator and the checker agree.
const AVG_CHAR_W = 0.50, LINE_H = 1.21;
const SLACK = 0.1;                // the auditor measures against (w - 0.1)

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

const FOOTER_OBJECTS = [
  { line: { x: 0, y: FOOT_RULE_Y, w: SW, h: 0,
            line: { color: RULE, width: 1 } } },
  { text: { text: "ETL4  |  DA 231o Data Engineering at Scale",
            options: { x: X, y: FOOT_RULE_Y + 0.06, w: 7.0, h: 0.32,
                       fontSize: PT_NOTE, color: MUTED, isTextBox: true,
                       margin: 0, valign: "middle", align: "left" } } },
  { slideNumber: { x: 11.80, y: FOOT_RULE_Y + 0.06, w: 0.78, h: 0.32,
                   fontSize: PT_NOTE, color: MUTED, align: "right", margin: 0 } },
];

// Only the title is a placeholder. A layout placeholder no slide fills is
// copied onto every slide as an empty "Click to add text" prompt box.
pres.defineSlideMaster({
  title: "CONTENT",
  background: { color: WHITE },
  objects: FOOTER_OBJECTS.concat([
    { line: { x: X, y: HEAD_RULE_Y, w: W, h: 0, line: { color: NAVY, width: 1.25 } } },
    { placeholder: {
        options: { name: "title", type: "title", x: X, y: TITLE_Y, w: W, h: TITLE_H,
                   fontSize: TITLE_PT, bold: true, color: NAVY, align: "left",
                   valign: "middle", margin: 0 },
        text: "" } },
  ]),
});

pres.defineSlideMaster({
  title: "PLAIN",
  background: { color: WHITE },
  objects: FOOTER_OBJECTS.slice(),
});

/* ------------------------------------------------------------- primitives */

let uid = 0;
const nm = (p) => p + "-" + (++uid);

// A 32 pt title that wraps steals the first content line, so refuse it here
// rather than let the geometry audit find the overlap later.
function slideTitle(slide, text) {
  if (wrapLines(text, TITLE_PT, W - SLACK) > 1) {
    throw new Error(`title wraps at ${TITLE_PT}pt (${text.length} chars): "${text}"`);
  }
  slide.addText(text, { placeholder: "title" });
}

function hrule(slide, y, x, w, color, width) {
  slide.addShape(pres.ShapeType.line, {
    x: x === undefined ? X : x, y: y, w: w === undefined ? W : w, h: 0,
    line: { color: color || RULE, width: width || 1 }, objectName: nm("rule"),
  });
}

// Lead paragraph under the title. Returns the y where the next block starts.
function lead(slide, text, opts) {
  const o = opts || {};
  const w = o.w || W;
  const h = textH(text, PT_BODY, w - SLACK) + 0.04;
  slide.addText(text, {
    x: o.x || X, y: o.y === undefined ? TOP : o.y, w: w, h: h,
    fontSize: PT_BODY, color: BODY, align: "left", isTextBox: true, margin: 0,
    lineSpacingMultiple: 1.18, objectName: nm("lead"),
  });
  return (o.y === undefined ? TOP : o.y) + h + (o.gap === undefined ? 0.30 : o.gap);
}

// 22 pt bold section heading inside a slide.
function sectionHead(slide, text, x, y, w) {
  const h = textH(text, PT_SECTION, (w || W) - SLACK) + 0.04;
  slide.addText(text, {
    x: x, y: y, w: w, h: h, fontSize: PT_SECTION, bold: true, color: NAVY,
    align: "left", isTextBox: true, margin: 0, objectName: nm("sechead"),
  });
  return y + h + 0.16;
}

const BLOCK_GAP = 0.09;      // subheading to its body
const SUB_LH = 1.12;

function blockHeight(b, w) {
  const inner = w - SLACK;
  let h = textH(b.head, PT_SUB, inner, SUB_LH) + BLOCK_GAP;
  if (b.body) h += textH(b.body, PT_BODY, inner, 1.18);
  return h;
}

// 18 pt bold subheading over 14 pt body. No container, no fill.
function block(slide, b, x, y, w) {
  const inner = w - SLACK;
  const hh = textH(b.head, PT_SUB, inner, SUB_LH);
  slide.addText(b.head, {
    x: x, y: y, w: w, h: hh + 0.04, fontSize: PT_SUB, bold: true, color: NAVY,
    align: "left", isTextBox: true, margin: 0, lineSpacingMultiple: SUB_LH,
    objectName: nm("bh"),
  });
  let yy = y + hh + BLOCK_GAP;
  if (b.body) {
    const bh = textH(b.body, PT_BODY, inner, 1.18);
    slide.addText(b.body, {
      x: x, y: yy, w: w, h: bh + 0.04, fontSize: PT_BODY, color: BODY,
      align: "left", isTextBox: true, margin: 0, lineSpacingMultiple: 1.18,
      objectName: nm("bb"),
    });
    yy += bh;
  }
  return yy;
}

// Blocks side by side in equal columns. Throws if the row passes `bottom`.
function blockRow(slide, y, blocks, opts) {
  const o = opts || {};
  const gap = o.gap === undefined ? 0.46 : o.gap;
  const total = o.w || W;
  const cw = (total - gap * (blocks.length - 1)) / blocks.length;
  const h = Math.max.apply(null, blocks.map((b) => blockHeight(b, cw)));
  if (o.bottom !== undefined && y + h > o.bottom + 0.001) {
    throw new Error(`blockRow overflow: ends at ${(y + h).toFixed(2)}in, limit ` +
                    `${o.bottom.toFixed(2)}in [first head: ${blocks[0].head}]`);
  }
  blocks.forEach((b, i) => block(slide, b, (o.x || X) + i * (cw + gap), y, cw));
  return y + h + (o.after === undefined ? 0.30 : o.after);
}

// Blocks stacked in one column, spread to fill down to `bottom`.
function blockColumn(slide, blocks, x, y, w, bottom, minGap) {
  minGap = minGap === undefined ? 0.26 : minGap;
  const hs = blocks.map((b) => blockHeight(b, w));
  const need = hs.reduce((a, b) => a + b, 0) + minGap * (blocks.length - 1);
  const avail = bottom - y;
  if (need > avail + 0.001) {
    throw new Error(`blockColumn overflow: needs ${need.toFixed(2)}in, has ` +
                    `${avail.toFixed(2)}in [first head: ${blocks[0].head}]`);
  }
  const slack = (avail - need) / Math.max(1, blocks.length - 1);
  let cy = y;
  blocks.forEach((b, i) => {
    block(slide, b, x, cy, w);
    cy += hs[i] + minGap + (i < blocks.length - 1 ? slack : 0);
  });
  return bottom;
}

// Numbered list: "1." in navy, then the subheading and the body. The items sit
// at a fixed rhythm; leftover height stays as whitespace rather than stretching
// the gaps, which would read as three unrelated blocks.
function numbered(slide, items, y, bottom, gap) {
  const NW = 0.44, tw = W - NW;
  const slack = gap === undefined ? 0.40 : gap;
  const hs = items.map((it) => blockHeight({ head: it.head, body: it.body }, tw));
  const need = hs.reduce((a, b) => a + b, 0) + slack * (items.length - 1);
  if (need > bottom - y + 0.001) {
    throw new Error(`numbered overflow: needs ${need.toFixed(2)}in, has ` +
                    `${(bottom - y).toFixed(2)}in`);
  }
  let cy = y;
  items.forEach((it, i) => {
    slide.addText(String(i + 1) + ".", {
      x: X, y: cy, w: NW, h: 0.32, fontSize: PT_SUB, bold: true, color: NAVY,
      align: "left", isTextBox: true, margin: 0, objectName: nm("num"),
    });
    block(slide, { head: it.head, body: it.body }, X + NW, cy, tw);
    cy += hs[i] + slack;
  });
  return cy;
}

// 11 pt footnote anchored to the bottom of the content area. A leading
// "Label:" is set in navy bold. Returns the top of the footnote.
function footnote(slide, lines) {
  const inner = W - SLACK;
  const h = lines.reduce((a, l) => a + textH(l, PT_NOTE, inner, 1.22), 0) + 0.05;
  const runs = [];
  lines.forEach((ln, i) => {
    const sep = ln.indexOf(":");
    const brk = i < lines.length - 1;
    if (sep === -1) {
      runs.push({ text: ln, options: { color: MUTED, breakLine: brk } });
    } else {
      runs.push({ text: ln.slice(0, sep + 1), options: { bold: true, color: NAVY } });
      runs.push({ text: ln.slice(sep + 1), options: { color: MUTED, breakLine: brk } });
    }
  });
  slide.addText(runs, {
    x: X, y: BOT - h, w: W, h: h, fontSize: PT_NOTE, color: MUTED, align: "left",
    isTextBox: true, margin: 0, lineSpacingMultiple: 1.22, objectName: nm("footnote"),
  });
  return BOT - h;
}

function bullets(slide, items, o) {
  const w = o.w || W;
  const h = items.reduce((a, t) => a + textH(t, PT_BODY, w - 0.42, 1.18) + 0.13, 0);
  slide.addText(items.map((t, i) => ({
    text: t, options: { bullet: true, breakLine: i < items.length - 1 },
  })), {
    x: o.x || X, y: o.y, w: w, h: h, fontSize: PT_BODY, color: BODY, align: "left",
    isTextBox: true, margin: 0, paraSpaceAfter: 9, lineSpacingMultiple: 1.18,
    objectName: nm("bullets"),
  });
  return o.y + h;
}

function tableRows(rows, cfg) {
  cfg = cfg || {};
  return rows.map((r, i) => r.map((c, j) => {
    const strong = i > 0 && cfg.boldFn && cfg.boldFn(i, j, c);
    return { text: c, options: {
      bold: i === 0 || Boolean(strong),
      color: i === 0 ? WHITE : (strong ? NAVY : BODY),
      fontSize: cfg.size || PT_BODY,
      fill: { color: i === 0 ? NAVY : (i % 2 ? WHITE : ZEBRA) },
      align: "left",
      valign: "middle",
    } };
  }));
}

const chartBase = {
  catAxisLabelColor: MUTED, valAxisLabelColor: MUTED,
  catAxisLabelFontSize: PT_NOTE, valAxisLabelFontSize: PT_NOTE,
  catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt",
  dataLabelFontFace: "+mn-lt", dataLabelFontSize: PT_NOTE,
  valGridLine: { color: "E8EBEF", size: 1 },
  catGridLine: { style: "none" },
  titleFontFace: "+mn-lt", titleFontSize: PT_BODY, titleColor: NAVY,
  legendFontFace: "+mn-lt", legendFontSize: PT_NOTE, legendColor: MUTED,
  valAxisTitleFontSize: PT_NOTE, valAxisTitleColor: MUTED,
  valAxisTitleFontFace: "+mn-lt",
  chartColors: [SER1, SER2, SER3],
  barDir: "col", barGrouping: "clustered", barGapWidthPct: 55,
  showLegend: true, legendPos: "b", showTitle: true,
};

/* ====================================================================== */
/*                                 DECK                                   */
/* ====================================================================== */

/* 1 --------------------------------------------------------------- title */
pres.addSection({ title: "Opening" });
let s = pres.addSlide({ masterName: "PLAIN", sectionTitle: "Opening" });
s.addText("DA 231o  |  PROJECT PRESENTATION", {
  x: X, y: 1.70, w: W, h: 0.30, fontSize: PT_BODY, color: MUTED, charSpacing: 1.5,
  align: "left", isTextBox: true, margin: 0, objectName: "t-eyebrow" });
s.addText("Scalable Kinematic Flight Trajectory\nReconstruction from Noisy ADS-B Data", {
  x: X, y: 2.14, w: W, h: 1.30, fontSize: TITLE_PT, bold: true, color: NAVY,
  align: "left", isTextBox: true, margin: 0, lineSpacingMultiple: 1.14,
  objectName: "t-title" });
hrule(s, 3.64, X, 3.40, NAVY, 1.75);
s.addText("Accuracy and cost trade-offs for three reconstruction methods, measured on a twelve-cell experiment matrix", {
  x: X, y: 3.86, w: 10.4, h: 0.56, fontSize: PT_SUB, color: BODY, align: "left",
  isTextBox: true, margin: 0, lineSpacingMultiple: 1.18, objectName: "t-sub" });
s.addText([
  { text: "Team ETL4", options: { bold: true, color: NAVY, breakLine: true } },
  { text: "Kompalli Avinash Bhargav  |  Nadhiya R S  |  Kasi Annapurna Uppalapati  |  Sathyanarayanan C",
    options: { color: BODY, breakLine: true } },
  { text: "Department of Computational and Data Sciences, Indian Institute of Science, Bangalore  |  November 2026",
    options: { color: MUTED } },
], { x: X, y: 4.82, w: W, h: 1.10, fontSize: PT_BODY, align: "left",
     isTextBox: true, margin: 0, lineSpacingMultiple: 1.30, objectName: "t-authors" });
s.addNotes("This is a course project for DA 231o. The deliverable has two parts: a lakehouse pipeline, and a measured comparison of three reconstruction methods.");

/* 2 ------------------------------------------------- the stream problem */
pres.addSection({ title: "Problem" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
slideTitle(s, "ADS-B gives position reports, not flights");
let y = lead(s, "Aircraft broadcast a position about once each second. OpenSky collects these broadcasts from several thousand receivers. The result is one mixed stream, and it does not mark flight boundaries.");
const fn2 = footnote(s, [
  "Source: OpenSky Network monthly flight-record archives 2019 to 2022, daily state-vector archives, and the live REST feed.",
]);
numbered(s, [
  { head: "Order the stream",
    body: "Group the reports by aircraft. Sort them by observation time." },
  { head: "Find the boundaries",
    body: "Find where one flight ends and the next one starts. One airframe flies several flights in a day." },
  { head: "Repair the path",
    body: "Make the trajectory continuous and physically possible. Some points are absent, and some values are corrupt." },
], y, fn2 - 0.22);
s.addNotes("Reports arrive out of order, so arrival time is not the event time. Receivers that overlap send duplicate points. The raw stream does not separate the flights of one airframe. The raw stream is not the deliverable. Every stage after it converts the stream into flights. Scope: convert the stream into trajectories at scale, implement three methods for the boundary problem, and measure the accuracy and the cost of each one.");

/* 3 --------------------------------------------- why the time rule fails */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
slideTitle(s, "The time-gap rule misreads gaps in both directions");
y = lead(s, "The usual rule splits a flight when two consecutive observations are more than 30 minutes apart. The rule reads elapsed time only. It ignores the motion that the two observations imply.");
const fn3 = footnote(s, [
  "Resolution: kinematics correct both cases. Speed, altitude rate and turn rate show whether the implied motion is physically possible.",
]);
y = blockRow(s, y, [
  { head: "A long gap is not always a new flight",
    body: "Two aircraft give the same 40-minute gap. The first one parks between legs. The second one cruises, but loses receiver coverage. The rule splits both. One split is wrong." },
  { head: "A short gap is not always continuous",
    body: "Two reports three seconds apart imply a 200 km jump. That motion is impossible, so one report is corrupt. The rule accepts both, because the time gap is small." },
], { after: 0.42 });
hrule(s, y, X, 2.60, NAVY, 1.5);
s.addText("RESEARCH QUESTION", {
  x: X, y: y + 0.14, w: W, h: 0.26, fontSize: PT_NOTE, bold: true, color: MUTED,
  charSpacing: 1.5, align: "left", isTextBox: true, margin: 0, objectName: "rq-label" });
s.addText("How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?",
  { x: X, y: y + 0.46, w: 11.0, h: 0.62, fontSize: PT_SUB, bold: true, color: NAVY,
    align: "left", isTextBox: true, margin: 0, lineSpacingMultiple: 1.18,
    objectName: "rq" });

/* 4 ----------------------------------------------- goals and targets */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
slideTitle(s, "Seven design goals, six performance targets");
const colW = (W - 0.70) / 2, colR = X + colW + 0.70;
let yl = sectionHead(s, "Design goals", X, TOP, colW);
bullets(s, [
  "Ingest historical archives and a live streaming feed",
  "Handle missing, duplicated, irregular and out-of-order records",
  "Reconstruct with a time baseline and two kinematic methods",
  "Extract speed, heading, acceleration, turn and altitude rate",
  "Detect significant deviations from a reference trajectory",
  "Generate a smoothed trajectory where the input is noisy",
  "Measure accuracy and cost together, not in isolation",
], { x: X, y: yl, w: colW });

let yr = sectionHead(s, "Performance targets", colR, TOP, colW);
bullets(s, [
  "Throughput in records per second",
  "End-to-end processing time and streaming latency",
  "CPU and memory utilisation",
  "Shuffle and partitioning overhead",
  "Scaling efficiency as data volume grows",
  "Robustness to missing, duplicated and out-of-order data",
], { x: colR, y: yr, w: colW });
footnote(s, [
  "Constraints: a fixed seed makes every stage reproducible. An append-only decision log records each schema change and each parameter change.",
]);
s.addNotes("The seven goals and the six targets come from the project proposal. Target five, scaling efficiency, constrained the architecture more than the other five combined.");

/* 5 ------------------------------------------- scaling efficiency drives */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Problem" });
slideTitle(s, "Scaling efficiency decided the architecture");
y = lead(s, "Scaling efficiency asks one question. Does cost grow with the new data, or with the total data? A pipeline that rebuilds all data on each run costs the square of the dataset. No per-stage optimisation corrects this.");
const fn5 = footnote(s, [
  "Setup: the warehouse build took 1031 s for three months of flight records. Month one also carries one global build of the aircraft hub, the identity satellite and the global flight-to-aircraft link.",
]);
blockRow(s, y, [
  { head: "Before: full rebuild",
    body: "Each run rebuilt every tier below bronze. N months of data therefore cost the sum of 1 to N months of work. A scale-out attempt filled the disk with about 20 GB of temporary files." },
  { head: "After: incremental refresh",
    body: "One added month costs one month of work. The measured marginal cost is 131 s for the second added month and 148 s for the third: the same order, with no growth." },
], { bottom: fn5 - 0.24 });
s.addNotes("Wall time against volume on the full-rebuild design shows rework only, so it hides scaling efficiency. The partition-commit markers make the marginal cost of an added month a direct measurement, not an average over the whole run. The global build took 11 minutes of the 1031 s run.");

/* 6 -------------------------------------------------------- architecture */
pres.addSection({ title: "Approach" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Two ingest paths, three storage tiers");
y = lead(s, "Each tier writes its output to disk, so a downstream tier rebuilds without a repeat of the work above it.");

const tiers = [
  { t: "OpenSky Network", d: "monthly flight archives\ndaily state archives\nlive REST feed" },
  { split: [
      { t: "Batch ingest", d: "archives to bronze\nno broker" },
      { t: "Apache Kafka", d: "live feed only\nkey = airframe id" },
    ] },
  { t: "Bronze", d: "source schema kept\nno projection\n121.6 M rows, 6.2 GB" },
  { t: "Silver", d: "Data Vault 2.0\nhubs, links, satellites\n225.5 M rows, 15 GB" },
  { t: "Gold", d: "dimensional marts\nfacts and dimensions\n19.0 M rows, 2.0 GB" },
  { t: "Experiments", d: "3 methods x 4 profiles\n12 cells, two inputs\nPostgres + Grafana" },
];
const cw6 = 1.78, gap6 = 0.232, tierH = 2.05;
// The second column splits: the archives reach bronze with no broker, so a
// single chain of boxes would state an ingest path the code does not have.
const subH = tierH / 2 - 0.05;
const subMid = [subH / 2, tierH / 2 + 0.05 + subH / 2];
tiers.forEach((tr, i) => {
  const x = X + i * (cw6 + gap6);
  const boxes = tr.split
    ? [{ t: tr.split[0].t, d: tr.split[0].d, by: y, bh: subH },
       { t: tr.split[1].t, d: tr.split[1].d, by: y + tierH / 2 + 0.05, bh: subH }]
    : [{ t: tr.t, d: tr.d, by: y, bh: tierH }];
  boxes.forEach((b, k) => {
    // Size the label box to its own text so the detail always starts directly
    // under it; a fixed height leaves a gap under every one-line label.
    const labH = (wrapLines(b.t, PT_BODY, cw6 - 0.34) * PT_BODY * 1.10) / 72 + 0.03;
    s.addShape(pres.ShapeType.rect, {
      x: x, y: b.by, w: cw6, h: b.bh, fill: { color: WHITE },
      line: { color: NAVY, width: 1 }, objectName: `tier${i}_${k}` });
    s.addText(b.t, {
      x: x + 0.12, y: b.by + 0.09, w: cw6 - 0.24, h: labH, fontSize: PT_BODY,
      bold: true, color: NAVY, align: "left", valign: "top", isTextBox: true,
      margin: 0, lineSpacingMultiple: 1.10, objectName: `tiert${i}_${k}` });
    s.addText(b.d, {
      x: x + 0.12, y: b.by + 0.14 + labH, w: cw6 - 0.24, h: b.bh - 0.23 - labH,
      fontSize: PT_NOTE, color: MUTED, align: "left", valign: "top",
      isTextBox: true, margin: 0, lineSpacingMultiple: 1.26,
      objectName: `tierd${i}_${k}` });
  });
  if (i < tiers.length - 1) {
    const mids = (tr.split || tiers[i + 1].split) ? subMid : [tierH / 2];
    mids.forEach((cy, k) => {
      s.addText(">", {
        x: x + cw6, y: y + cy - 0.18, w: gap6, h: 0.36, fontSize: PT_BODY,
        bold: true, color: MUTED, align: "center", valign: "middle",
        isTextBox: true, margin: 0, objectName: `arrow${i}_${k}` });
    });
  }
});
const fn6 = footnote(s, [
  "Paths: Kafka carries the live REST feed only. The consumer writes it beside the daily state files. The batch loaders write the archives to bronze with no broker.",
  "Deployment: Docker Compose runs nine services on one workstation. The Spark cluster has one master and two workers.",
]);
const ARCH_WHY = "Repeated work is the largest cost in this pipeline. A single tier has two penalties. Each schema change forces a new pull from OpenSky, and OpenSky limits the request rate. Each metric change forces a new cleaning pass, the most expensive stage. Separate tiers remove both penalties.";
lead(s, ARCH_WHY, { y: y + tierH + 0.34, gap: 0 });
if (y + tierH + 0.34 + textH(ARCH_WHY, PT_BODY, W - SLACK, 1.18) > fn6 - 0.18) {
  throw new Error("slide 6: the closing paragraph runs into the footnote");
}

/* 7 ------------------------------------------------------- data vault 2.0 */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Data Vault 2.0 reconciles three sources");
y = lead(s, "Hubs hold identity. Links hold relationships. Satellites hold attributes, and take new rows only.");

const TBLW = 6.05;
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
]),
  { x: X, y: y, w: TBLW, colW: [1.08, 1.70, 1.97, 1.30], rowH: 0.34,
    border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 8, 3, 8],
    objectName: "vault-table" });

const fn7 = footnote(s, [
  "Keys: the flight hub uses callsign and date, not airframe, because one airframe flies several flights in a day. Hash keys replace natural keys in every join.",
]);
blockColumn(s, [
  { head: "Multi-source reconciliation",
    body: "Three sources observe the same aircraft. The builder creates one hub row for each key, and a later observation attaches to that row." },
  { head: "Additive schema evolution",
    body: "A new attribute arrives as a new satellite on an existing hub. No existing table changes." },
  { head: "Per-source lineage",
    body: "Every row records its ingest path and its load time. Without this record, no tier replays on its own." },
], X + TBLW + 0.50, y, W - TBLW - 0.50, fn7 - 0.20);
s.addNotes("Every row has four lineage columns: business-key hash, load timestamp, record-source tag, and partition month. The builder never updates a satellite in place. An update destroys the history that the load timestamp records. A schema change therefore cannot break a working stage.");

/* 8 ---------------------------------------------- partitioning mechanics */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Monthly partitions make scaling measurable");
y = lead(s, "Each time-sensitive table uses the calendar month as its partition. A watermark registry records the last complete month for each tier and each table.");
const fn8 = footnote(s, [
  "Contract: one partition is one file at ym=YYYY-MM/part.parquet. A byte-level hash of every output partition matched across two runs.",
]);
blockColumn(s, [
  { head: "The write is atomic",
    body: "The writer puts a partition in a temporary path, flushes it, then renames it into place. A parallel reader therefore sees the complete old partition or the complete new one." },
  { head: "The registry advances with the write",
    body: "The watermark moves in the same transaction as the partition write. The registry therefore never runs ahead of the data." },
  { head: "A completion marker bounds the restart",
    body: "The marker records the last complete partition, so a stopped run resumes at a partition boundary instead of at the start." },
], X, y, W, fn8 - 0.24, 0.30);
s.addNotes("The marginal cost of an added month is 131 s and 148 s for the second and third month. Before the partition contract, each run rebuilt every tier below bronze.");

/* 9 ------------------------------------------------------------- kafka */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Kafka decouples the live feed from the write");
y = lead(s, "The project chose Kafka over a task-queue broker, because a broker optimises acknowledgement, not replayable streams.");
const fn9 = footnote(s, [
  "Dead-letter policy: the producer diverts a record with a malformed array shape, and a record that fails to serialise, and it keeps the failure reason.",
]);
blockRow(s, y, [
  { head: "Topics and partitions",
    body: "The raw topic has three partitions, one for each consumer worker. The dead-letter topic has one, which keeps global order for failure analysis." },
  { head: "The key is the airframe identifier",
    body: "Every observation of one aircraft goes to the same partition, and one consumer reads them in order." },
  { head: "At-least-once, by decision",
    body: "The consumer commits offsets after a batch reaches storage. A crash replays the open window, and silver absorbs the duplicates." },
], { bottom: fn9 - 0.24 });
s.addNotes("Without the airframe key, two consumers corrupt the load-timestamp order that deduplication needs. Exactly-once delivery solves a problem that the data model solves already. A direct write from the API to storage was the other option. It couples the poll rate to the write rate, and it loses data whenever the write path stalls. The producer retries a failed poll on the next interval, so a short outage costs one window, not the run.");

/* 10 --------------------------------------------------- raw ingest rule */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "The producer publishes all 17 source fields");
y = lead(s, "A projection at ingest loses the raw record. A later stage then has no way to read a field that the projection dropped. The clean stage does the projection instead, in one function.");
const fn10 = footnote(s, [
  "Convergence: one projection function maps the REST, archive and synthetic schemas onto the canonical schema, so the three source schemas meet in one place.",
]);
blockRow(s, y, [
  { head: "Bronze keeps the source schema",
    body: "Bronze stores every source field as received. No rename, no type coercion, no projection." },
  { head: "The token refreshes early",
    body: "The OAuth2 token refreshes 30 seconds before it expires, so a long run never stops in mid-poll." },
  { head: "Three metrics leave the producer",
    body: "Prometheus scrapes records published, records sent to the dead-letter topic by reason, and publish latency." },
], { bottom: fn10 - 0.24 });
s.addNotes("This is a Data Engineering at Scale discipline: raw ingest stays raw, so a downstream decision can be revisited without a fresh pull from OpenSky.");

/* 11 --------------------------------------------------- platform stack */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Each platform answers one decisive property");
const fn11 = footnote(s, [
  "Kafka: the two previous slides cover the streaming path and the raw-ingest rule.",
]);
s.addTable(tableRows([
  ["Platform", "Role in the pipeline", "Why this one"],
  ["Apache Spark", "Distributed build of the state-observation satellite",
   "The only table whose source volume exceeds single-host memory"],
  ["PostgreSQL", "Telemetry, per-metric results, watermark registry, dashboard tables",
   "Constraints reject an invalid watermark at the storage boundary"],
  ["Prometheus, Grafana", "Scrapes Spark, host and producer metrics; 68 panels across 8 dashboards",
   "The six performance targets map onto counters, histograms and gauges"],
  ["Apache Airflow", "Schedules the streaming path, branches live against batch, retries",
   "A script runs the batch path, because a script is simpler to time per stage"],
  ["Great Expectations", "25 expectations across 5 suites at the bronze and mart boundaries",
   "The suites are declarative, and one failure blocks promotion of that table"],
  ["Apache Parquet", "Columnar storage with a dataset abstraction",
   "Partition predicates push into file listing; an explicit type keeps a hex id a string"],
  ["Docker Compose", "Runs the nine-service stack on one host",
   "The target is one workstation, where a cluster control plane adds cost only"],
]),
  { x: X, y: TOP, w: W, colW: [1.95, 4.45, 5.43], rowH: 0.38,
    border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 8, 3, 8],
    objectName: "platform-table" });
s.addNotes("Airflow orchestrates the streaming path. A dedicated script runs the batch path for the evaluation measurements, because per-stage timing is simpler to instrument in a script.");

/* 12 ----------------------------------------------------- three methods */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Three methods span a wide cost range");
y = lead(s, "The project implements three methods, not two. A cost-accuracy curve needs three points to show its shape. The three methods also match three families in the aviation literature.");
const fn12 = footnote(s, [
  "Thresholds: the code fixes the five values in Method B. It does not derive them per aircraft type.",
]);
blockRow(s, y, [
  { head: "A.  Time-gap baseline",
    body: "Observations group into one flight until the gap exceeds 1,800 seconds. This threshold follows OpenSky practice for commercial operations. The method reads no kinematic feature." },
  { head: "B.  Kinematic segmentation",
    body: "A rule breaks the flight on five signals:\ngap over 1,800 s\nposition-jump flag\naltitude rate over 50 m/s\nturn rate over 30 deg/s\ncallsign change in flight" },
  { head: "C.  Kinematic with Kalman",
    body: "The same segmenter runs with the position-jump break off. A six-state constant-velocity Kalman filter then runs for each flight in a local East-North-Up frame. A chi-squared gate tests the residual." },
], { bottom: fn12 - 0.24, gap: 0.52 });
s.addNotes("The project uses a constant-velocity model, not a constant-acceleration or coordinated-turn model. A higher-order model needs more observations in each segment. The research question asks what a smoother adds, and the simplest filter isolates that contribution. Segments with fewer than two observations pass through unchanged.");

/* 13 ------------------------------------------------- rule versus gate */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Method C replaces the jump rule with a gate");
y = lead(s, "The jump rule and the innovation gate treat the same condition: an observation whose implied motion is impossible. They differ in the result. Only one of the two acts on a row, so Method C turns the rule off.");
const fn13 = footnote(s, [
  "Outcome: the evaluation decides between the two. Slide 20 reports the flight count that each choice produces.",
]);
blockRow(s, y, [
  { head: "The rule starts a new flight",
    body: "A position-jump flag ends the segment. The method keeps the observation, and cuts the trajectory at that row. A corrupt row therefore becomes a flight boundary." },
  { head: "The gate lowers the weight",
    body: "The filter compares the residual against a chi-squared threshold. It then treats a measurement beyond the threshold as corrupt, and the flight continues through it." },
], { bottom: fn13 - 0.24 });
s.addNotes("The position-jump flag is computed during cleaning from the implied straight-line speed between consecutive observations, against a threshold of 340 m/s.");

/* 14 --------------------------------------------- noise ladder + design */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Approach" });
slideTitle(s, "Four profiles, twelve cells, one fixed seed");
y = lead(s, "Real ADS-B has no ground truth, so absolute position error needs a synthetic input. Four profiles apply the same four faults at rising rates.");
const fn14 = footnote(s, [
  "Injector: a position jump offsets latitude and longitude by up to 2 degrees. The code defines no Gaussian position or altitude jitter in these four profiles.",
  "Metrics per cell: segmentation precision, recall and F1 within a 5 s tolerance; boundary time error; trajectory continuity; coverage; position RMSE; deviation-detection precision, recall and F1.",
]);
const NTW = 6.05;
s.addTable(tableRows([
  ["Profile", "Dropped rows", "Duplicates", "Reordered", "Position jumps"],
  ["N0", "0%", "0%", "0%", "0%"],
  ["N1", "2%", "0.5%", "1%", "0.1%"],
  ["N2", "5%", "1%", "3%", "0.5%"],
  ["N3", "10%", "2%", "10%", "1%"],
]),
  { x: X, y: y, w: NTW, colW: [0.95, 1.55, 1.25, 1.20, 1.10], rowH: 0.34,
    border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 8, 3, 8],
    objectName: "noise-table" });
blockColumn(s, [
  { head: "Input 1: synthetic, known truth",
    body: "Each cell has 40 flights and 10 injected deviations. It answers the accuracy half, because position error and flight count are measurable." },
  { head: "Input 2: real OpenSky observations",
    body: "4,361,653 rows per cell. This input has no truth, but it has operational volume, so it answers the cost half." },
], X + NTW + 0.50, y, W - NTW - 0.50, fn14 - 0.20);
s.addText("N0 sets the correctness floor. A method that does not reach near-zero error on clean input is broken, and the matrix detects that.",
  { x: X, y: y + 1.96, w: NTW, h: 0.80, fontSize: PT_BODY, color: BODY, align: "left",
    isTextBox: true, margin: 0, lineSpacingMultiple: 1.18, objectName: "n0-note" });
s.addNotes("Three methods by four profiles gives twelve cells. Each cell runs against both inputs, with seed 42. A uniqueness constraint over month, source, method and noise makes a repeat run idempotent, so every figure traces to one execution and not to the best of several. The injector applies the four operations in one order: row removal, duplicate insertion, position jump, out-of-order permutation.");

/* 15 ---------------------------------------------- RESULTS divider */
pres.addSection({ title: "Results" });
s = pres.addSlide({ masterName: "PLAIN", sectionTitle: "Results" });
s.addText("EVALUATION", {
  x: X, y: 2.90, w: W, h: 0.30, fontSize: PT_BODY, bold: true, color: MUTED,
  charSpacing: 2, align: "left", isTextBox: true, margin: 0, objectName: "d-eyebrow" });
s.addText("What the twelve-cell matrix measured", {
  x: X, y: 3.28, w: W, h: 0.62, fontSize: TITLE_PT, bold: true, color: NAVY,
  align: "left", isTextBox: true, margin: 0, objectName: "d-title" });
hrule(s, 4.08, X, 3.40, NAVY, 1.75);
s.addText("Accuracy on synthetic input with known truth, cost on 4.36 million real observations, and the answer to the research question.",
  { x: X, y: 4.30, w: 9.6, h: 0.56, fontSize: PT_SUB, color: BODY, align: "left",
    isTextBox: true, margin: 0, lineSpacingMultiple: 1.18, objectName: "d-sub" });

/* 16 ----------------------------------------------- accuracy vs cost plot */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
slideTitle(s, "Time-gap and kinematic differ in cost only");
const fn16 = footnote(s, [
  "Setup: synthetic input, 14,400 observations per cell, truth 40 flights. Wall time is per cell on the deployment host. The shaded band marks RMSE at or below 0.001 m.",
  "Result: the two unsmoothed methods give the same error at every profile. The smoothed method costs an order of magnitude more, and at N0, N1 and N3 its error falls below the measurable floor.",
]);
const imgH = fn16 - TOP - 0.22, imgW = imgH * (6.4 / 4.0);
s.addImage({ path: path.join(FIG, "fig1_accuracy_cost.png"),
  x: X, y: TOP, w: imgW, h: imgH, objectName: "fig1" });

/* 17 --------------------------------------------------- over-fragmentation */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
slideTitle(s, "Kinematic segmentation over-fragments flights");
const fn17 = footnote(s, [
  "Result: the Kalman-smoothed method holds 18,458 to 18,913 flights across the four profiles. The baseline gives 14,946 to 14,949.",
]);
const CHW = 6.30;
s.addChart([{
  type: pres.ChartType.bar,
  data: [
    { name: "Time-gap", labels: ["N0", "N1", "N2", "N3"], values: [14949, 14948, 14946, 14946] },
    { name: "Kinematic", labels: ["N0", "N1", "N2", "N3"], values: [102169, 108097, 134957, 163241] },
    { name: "Kinematic + Kalman", labels: ["N0", "N1", "N2", "N3"], values: [18913, 18814, 18679, 18458] },
  ],
}], Object.assign({}, chartBase, {
  x: X, y: TOP, w: CHW, h: fn17 - TOP - 0.20,
  title: "Flights reconstructed from 4.36 M real observations",
  valAxisTitle: "Flights", showValAxisTitle: true,
  objectName: "chart-frag",
}));
blockColumn(s, [
  { head: "Worse on real data than on synthetic",
    body: "On clean synthetic input the rule finds the true 40 flights. It then gives 68, 175 and 285 as noise rises. On real data it fragments at N0 already: 102,169 flights against 14,949 for the baseline." },
  { head: "Why",
    body: "A natural gap in receiver coverage looks the same to the rule as injected noise. The rule operates correctly. The input causes the result." },
  { head: "It costs the deviation metric too",
    body: "At N2 the method flags 2 events against 10 injected events. Each fragment is too short to hold one deviation." },
], X + CHW + 0.50, TOP, W - CHW - 0.50, fn17 - 0.20);

/* 18 ------------------------------------------------------- cost at scale */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
slideTitle(s, "The smoother costs six times the baseline");
const fn18 = footnote(s, [
  "Setup: each cell uses 4,361,653 real OpenSky state vectors on the deployment host. Real input has no ground truth, so this slide reports cost only.",
]);
s.addChart([{
  type: pres.ChartType.bar,
  data: [
    { name: "Time-gap", labels: ["N0", "N1", "N2", "N3"], values: [60.0, 59.4, 54.4, 56.1] },
    { name: "Kinematic", labels: ["N0", "N1", "N2", "N3"], values: [201.3, 215.8, 257.1, 312.5] },
    { name: "Kinematic + Kalman", labels: ["N0", "N1", "N2", "N3"], values: [353.7, 351.7, 335.1, 323.3] },
  ],
}], Object.assign({}, chartBase, {
  x: X, y: TOP, w: CHW, h: fn18 - TOP - 0.20,
  title: "Wall time per 4,361,653 real observations",
  valAxisTitle: "Seconds", showValAxisTitle: true,
  objectName: "chart-cost",
}));
const TPX = X + CHW + 0.50, TPW = W - CHW - 0.50;
s.addTable(tableRows([
  ["Method", "Throughput (rec/s)", "vs base"],
  ["Time-gap", "72,732 to 80,247", "1.0x"],
  ["Kinematic", "13,958 to 21,670", "3.4x to 5.6x"],
  ["Kinematic + Kalman", "12,331 to 13,490", "5.8x to 6.2x"],
]),
  { x: TPX, y: TOP, w: TPW, colW: [1.95, 1.90, 1.23], rowH: 0.42,
    border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 8, 3, 8],
    objectName: "tput-table" });
blockColumn(s, [
  { head: "The two methods scale differently",
    body: "Baseline throughput stays flat. Kinematic throughput falls from 21,670 to 13,958 as noise rises. The segment count drives its cost, and that count grows." },
  { head: "The smoother rises slightly",
    body: "The filter step dominates its cost, and a heavier profile drops observations." },
], TPX, TOP + 2.10, TPW, fn18 - 0.20);

/* 19 ------------------------------------------------------ accuracy matrix */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
slideTitle(s, "Accuracy comes from estimation, not boundaries");
const fn19 = footnote(s, [
  "Truth: each cell has 40 flights and 10 injected deviations.",
  "Caveat: the synthetic generator uses the same constant-velocity model as the filter. Sub-millimetre recovery is therefore an upper bound for a real aircraft in a manoeuvre.",
]);
const MTW = 6.55;
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
  ["Kin. + Kalman", "N0", "1.416", "< 0.001", "40", "10"],
  ["Kin. + Kalman", "N1", "1.317", "< 0.001", "40", "10"],
  ["Kin. + Kalman", "N2", "1.268", "1,445,595.8", "40", "11"],
  ["Kin. + Kalman", "N3", "1.196", "< 0.001", "40", "10"],
], {
  size: 12,
  // Rule 18: highlight the two conclusions the table carries, nothing else.
  boldFn: (i, j, c) => (j === 4 && c !== "40") || (j === 3 && c === "< 0.001") ||
                       (j === 3 && c === "1,445,595.8"),
}), { x: X, y: TOP, w: MTW, colW: [1.60, 0.72, 0.88, 1.35, 0.90, 1.10], rowH: 0.315,
  border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 7, 3, 7],
  objectName: "matrix-table" });

blockColumn(s, [
  { head: "Segmentation and estimation separate",
    body: "Time-gap and kinematic report the same RMSE at every profile. Neither estimates a position." },
  { head: "The gate beats the rule",
    body: "Method C holds the flight count at truth in every profile. The jump rule gave 285 flights at N3." },
  { head: "One cell needs an explanation",
    body: "The N2 smoothed cell drifts. Its gate threshold comes from N1 residuals, and rejects valid N2 measurements. One value causes the fault." },
], X + MTW + 0.45, TOP, W - MTW - 0.45, fn19 - 0.16, 0.24);

/* 20 -------------------------------------------- six performance targets */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
slideTitle(s, "Six targets, each with an instrument");
const fn20 = footnote(s, [
  "Reading: two targets read as a dashboard series rather than as one headline number. The single-host path makes no shuffle by construction, not by optimisation.",
]);
s.addTable(tableRows([
  ["Target", "Instrumentation", "Measured result"],
  ["Throughput", "Per-cell row counts and wall time; producer counter scraped continuously",
   "72,700 to 80,200 rec/s baseline; 12,300 to 13,500 rec/s smoothed"],
  ["End-to-end time and latency", "Every stage persists its elapsed time; publish-latency histogram, 11 buckets",
   "1031 s warehouse build for 3 months, 112.2 s downstream"],
  ["CPU and memory", "Host and container gauges scraped continuously",
   "Resource dashboard"],
  ["Shuffle and partitioning", "Spark metrics endpoint reports shuffle bytes",
   "No shuffle on the single-host path"],
  ["Scaling efficiency", "Partition-commit timestamps, per month and per table",
   "131 s and 148 s for the second and third added month"],
  ["Robustness", "The N0 to N3 ladder on both inputs",
   "12 of 12 cells complete on each input"],
]),
  { x: X, y: TOP, w: W, colW: [2.55, 4.70, 4.58], rowH: 0.42,
    border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 8, 3, 8],
    objectName: "targets-table" });
s.addNotes("Of the 112.2 s downstream, the marts take 70.5 s and data quality takes 27.4 s. The matrix takes 13.3 s, and the evaluation store 0.8 s. Latency percentiles appear on the streaming dashboard. Scaling efficiency shaped the architecture: the partition-commit markers make the marginal cost of an added month a direct measurement.");

/* 21 ------------------------------------------------------------ delivery */
pres.addSection({ title: "Summary" });
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
slideTitle(s, "Delivery against the proposal");
y = lead(s, "The deployed system delivers all seven design goals, and it measures all six performance targets.");
const fn21 = footnote(s, [
  "Verification: three cross-tier identity checks hold exactly, so silver and gold lose no row and duplicate no row.",
]);
s.addTable(tableRows([
  ["Deliverable", "Result"],
  ["Raw data ingested", "14.1 GB, 121.6 M records across 48 months of archives"],
  ["Warehouse rows written", "244.5 M: 225.5 M silver, 19.0 M gold"],
  ["Experiment cells complete", "12 of 12, on each of the two inputs"],
  ["Automated tests", "92 collected across six suites; 20 pipeline invariants pass"],
  ["Data-quality expectations", "25 of 25 pass, across five suites"],
  ["Rollback scenarios", "7 of 7 pass, in downstream-first order"],
  ["Grafana dashboards provisioned", "8 dashboards, 68 panels"],
  ["Largest single optimisation", "17x, from the vectorised cross-track distance"],
]),
  { x: X, y: y, w: W, colW: [4.20, 7.63], rowH: 0.42,
    border: { type: "solid", color: RULE, pt: 1 }, margin: [3, 8, 3, 8],
    objectName: "delivery-table" });
s.addNotes("A byte-level hash of every output partition matched across two runs. The cross-track distance was the single largest optimisation in the project: profiling showed the per-row form consuming about 95 percent of matrix wall time.");

/* 22 ------------------------------------------------------------- answer */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
slideTitle(s, "The gain comes from the estimator");
y = lead(s, "How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?");
const fn22 = footnote(s, [
  "Frontier: the project reads the frontier within each noise profile. At N1 and N3 it holds the baseline and the smoothed method. At N0 and N2 it holds the baseline alone.",
]);
blockRow(s, y, [
  { head: "Kinematic segmentation alone gains nothing",
    body: "It gives the same position error as the baseline at every profile, because neither method estimates a position. Under noise it is worse at its own task: it splits 40 true flights into 285." },
  { head: "The estimator supplies the gain",
    body: "The Kalman filter cuts position error from kilometres to below a millimetre. It treats an implausible observation as a corrupt measurement, not as a flight boundary, and holds the flight count at truth." },
  { head: "The cost is about six times",
    body: "On 4.36 million real observations the smoother costs 5.8 to 6.2 times the baseline wall time. On the smaller synthetic input it costs 7.4 to 9.2 times." },
], { bottom: fn22 - 0.24, gap: 0.52 });
s.addNotes("On real data kinematic segmentation fragments with no injected noise at all. The baseline dominates kinematic segmentation at every profile: it costs less and delivers identical position error.");

/* 23 --------------------------------------------------- practical reading */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
slideTitle(s, "When the smoother earns its cost");
y = lead(s, "This answer is sharper than the proposal expected. The gain comes from estimation, not from kinematics in general.");
const fn23 = footnote(s, [
  "Qualification: the synthetic generator and the filter share a constant-velocity model, so sub-millimetre recovery is an upper bound. The cost figures on real data do not depend on that assumption.",
]);
blockRow(s, y, [
  { head: "Use the smoother",
    body: "The cost is worthwhile when the trajectory itself is the object of analysis, and the work needs metre-level positions. No cheaper method substitutes for it." },
  { head: "Use the baseline",
    body: "The cost is not worthwhile when the work needs flight boundaries only. The baseline gives those at a sixth of the cost, and kinematic segmentation gives no accuracy in return for its own cost." },
], { bottom: fn23 - 0.24 });

/* 24 ------------------------------------------------------- future work */
s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Summary" });
slideTitle(s, "Three extensions to the delivered system");
y = lead(s, "These three extensions appear in order of how little each one changes the delivered system.");
const fn24 = footnote(s, [
  "Attachments: source code, 92 automated tests, 8 Grafana dashboards, the append-only decision log, and the reproducibility fixtures.",
]);
numbered(s, [
  { head: "Move the remaining builders to Spark",
    body: "The monthly partition contract and the watermark semantics are engine-independent. Moving the flight-record builders to Spark changes the engine only. It is not a redesign." },
  { head: "Run the full 48-month archive",
    body: "The marginal cost is 131 to 148 seconds for each month. A run over the whole archive gives the frontier its full historical depth, and shows the seasonal change in traffic density." },
  { head: "Adopt a managed table format",
    body: "Apache Iceberg or Delta Lake adds snapshot isolation and time-travel queries, and replaces the custom watermark code. The current code follows the same semantics." },
], y, fn24 - 0.22);
s.addNotes("The partitioned build currently covers three of the 48 months. The remaining months stay in the pre-partitioning layout until an operator runs the migration.");

/* --------------------------------------------------------------- write */
const OUT = process.env.DECK_OUT ||
  path.join(__dirname, "ETL4_Project_Presentation.pptx");
(async () => {
  await pres.writeFile({ fileName: OUT });
  // pptxgenjs writes the two theme fonts but hard-codes Office's palette, so
  // every scheme color resolves to Office blue until a post-pass writes THEME
  // into ppt/theme/theme1.xml. Point APPLY_THEME_JS at that helper.
  if (process.env.APPLY_THEME_JS) {
    const { applyTheme } = require(process.env.APPLY_THEME_JS);
    await applyTheme(OUT, THEME);
  } else {
    console.log("APPLY_THEME_JS unset: scheme colors left on the pptxgenjs default palette");
  }
  console.log("wrote " + OUT);
})();
