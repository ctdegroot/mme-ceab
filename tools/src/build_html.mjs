import { readFileSync, writeFileSync } from "node:fs";

// Inject the verified core verbatim (strip module `export` keywords).
const core = readFileSync(new URL("validator_core.mjs", import.meta.url), "utf8").replace(/^export /gm, "");

const UI_JS = String.raw`
// ---------- UI wiring ----------
const $ = (sel) => document.querySelector(sel);
const dropzone = $("#dropzone");
const fileInput = $("#fileInput");
const results = $("#results");
const intro = $("#intro");

function iconFor(level) {
  return level === "error" ? "✕" : level === "warning" ? "!" : "i";
}
function esc(s) {
  return String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}

function summaryBanner(fileName, report) {
  const { errors, warnings, infos } = report;
  let cls, big, small;
  if (errors > 0) {
    cls = "b-error"; big = errors + " " + (errors === 1 ? "problem needs fixing" : "problems need fixing");
    small = warnings ? warnings + " other thing" + (warnings === 1 ? "" : "s") + " to double-check too." : "Please fix the item" + (errors === 1 ? "" : "s") + " below, then check the file again.";
  } else if (warnings > 0) {
    cls = "b-warn"; big = warnings + " thing" + (warnings === 1 ? "" : "s") + " to double-check";
    small = "No blocking problems — but please review the note" + (warnings === 1 ? "" : "s") + " below.";
  } else if (infos > 0) {
    cls = "b-info"; big = "Nothing to submit yet";
    small = "See the note below.";
  } else {
    cls = "b-ok"; big = "All good — this file is ready to submit!";
    small = "No problems found.";
  }
  return '<div class="banner ' + cls + '"><div class="banner-big">' + esc(big) +
    '</div><div class="banner-small">' + esc(small) + '</div></div>';
}

function issueCard(issue) {
  return '<div class="issue ' + issue.level + '">' +
    '<div class="issue-badge" aria-hidden="true">' + iconFor(issue.level) + '</div>' +
    '<div class="issue-body"><div class="issue-title">' + esc(issue.title) + '</div>' +
    '<div class="issue-detail">' + esc(issue.detail) + '</div></div></div>';
}

function renderResult(fileName, report) {
  const order = { error: 0, warning: 1, info: 2 };
  const sorted = report.issues.slice().sort((a, b) => order[a.level] - order[b.level]);
  const cards = sorted.map(issueCard).join("");
  return '<section class="result">' +
    '<div class="filename" title="' + esc(fileName) + '">📄 ' + esc(fileName) + '</div>' +
    summaryBanner(fileName, report) +
    (cards ? '<div class="issues">' + cards + '</div>' : '') +
    '</section>';
}

async function handleFiles(fileList) {
  const files = Array.from(fileList).filter((f) => /\.xlsx$/i.test(f.name));
  const skipped = Array.from(fileList).filter((f) => !/\.xlsx$/i.test(f.name));
  if (!files.length && !skipped.length) return;

  intro.style.display = "none";
  results.innerHTML = '<div class="working">Checking…</div>';

  const blocks = [];
  for (const file of files) {
    try {
      const ab = await file.arrayBuffer();
      const report = await validate(ab);
      blocks.push(renderResult(file.name, report));
    } catch (e) {
      blocks.push(renderResult(file.name, {
        issues: [{ level: "error", title: "Sorry — this file couldn’t be read",
          detail: "Please make sure it is a CEAB spreadsheet saved as .xlsx and isn’t open in Excel, then try again." }],
        errors: 1, warnings: 0, infos: 0, ok: false,
      }));
    }
  }
  for (const f of skipped) {
    blocks.push(renderResult(f.name, {
      issues: [{ level: "error", title: "This isn’t an Excel (.xlsx) file",
        detail: "Please choose the CEAB spreadsheet, saved as an Excel Workbook (.xlsx). Other file types can’t be checked." }],
      errors: 1, warnings: 0, infos: 0, ok: false,
    }));
  }
  results.innerHTML = blocks.join("") +
    '<div class="again"><button id="againBtn" type="button">Check another file</button></div>';
  document.getElementById("againBtn").addEventListener("click", reset);
  results.scrollIntoView({ behavior: "smooth", block: "start" });
}

function reset() {
  results.innerHTML = "";
  intro.style.display = "";
  fileInput.value = "";
  window.scrollTo({ top: 0, behavior: "smooth" });
}

// Drag & drop
["dragenter", "dragover"].forEach((ev) =>
  dropzone.addEventListener(ev, (e) => { e.preventDefault(); dropzone.classList.add("over"); }));
["dragleave", "drop"].forEach((ev) =>
  dropzone.addEventListener(ev, (e) => { e.preventDefault(); dropzone.classList.remove("over"); }));
dropzone.addEventListener("drop", (e) => handleFiles(e.dataTransfer.files));
dropzone.addEventListener("click", () => fileInput.click());
dropzone.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInput.click(); } });
fileInput.addEventListener("change", (e) => handleFiles(e.target.files));

// Guard: very old browsers without DecompressionStream
if (typeof DecompressionStream === "undefined") {
  intro.innerHTML = '<div class="issue error"><div class="issue-badge">!</div><div class="issue-body">' +
    '<div class="issue-title">Please use a newer browser</div>' +
    '<div class="issue-detail">This checker needs an up-to-date version of Chrome, Edge, Firefox, or Safari. ' +
    'Please update your browser (or try a different one) and open this page again.</div></div></div>';
  dropzone.style.display = "none";
}
`;

const BODY = String.raw`
<div class="wrap">
  <header>
    <h1>CEAB Data Checker</h1>
    <p class="tagline">Check your CEAB spreadsheet for problems before you submit it — and get plain-English instructions on how to fix anything that’s wrong.</p>
    <p class="privacy">🔒 Your file stays on your own computer. Nothing is uploaded or sent anywhere.</p>
  </header>

  <div id="dropzone" tabindex="0" role="button" aria-label="Drop your spreadsheet here or click to choose a file">
    <div class="dz-icon">📄⬆️</div>
    <div class="dz-main">Drag your spreadsheet here</div>
    <div class="dz-sub">or <span class="link">click to choose a file</span> &nbsp;·&nbsp; Excel (.xlsx)</div>
    <input type="file" id="fileInput" accept=".xlsx" multiple hidden />
  </div>

  <div id="intro">
    <h2>What this checks</h2>
    <ul class="checklist">
      <li><b>Course &amp; instructor details</b> — prefix, number, suffix, academic year, and year-in-program are valid and complete</li>
      <li><b>Every measurement</b> — attribute, indicator, deliverable type and name, and a real date on each row</li>
      <li><b>Grade scale set-up</b> — the right “maximum score” and threshold values are filled in for the scale you chose</li>
      <li><b>Score values</b> — no text in score cells, nothing above the maximum or outside the 1–4 scale</li>
      <li><b>Zeros</b> — flagged so you can confirm each was a real zero, not a “not completed”</li>
      <li><b>Student IDs</b> — the login/username, not a 9-digit student number, and no duplicates</li>
      <li><b>Layout</b> — no stray data outside the labelled columns, and every column matches a measurement (catches left-over columns from a previous year)</li>
    </ul>
    <p class="note">Fix anything marked with a red ✕ before submitting. Amber ! notes are worth a quick look. Then re-check the file — you can run this as many times as you like.</p>
  </div>

  <div id="results" aria-live="polite"></div>

  <footer>Runs entirely in your browser · no internet connection required once the page has loaded.</footer>
</div>
`;

const CSS = String.raw`
:root {
  --bg: #f4f6f9; --panel: #ffffff; --ink: #1a2230; --muted: #5b6675;
  --line: #e2e8f0; --accent: #2563eb; --accent-ink: #1d4ed8;
  --ok-bg:#ecfdf3; --ok-line:#12b76a; --ok-ink:#027a48;
  --err-bg:#fef3f2; --err-line:#f04438; --err-ink:#b42318;
  --warn-bg:#fffaeb; --warn-line:#f79009; --warn-ink:#b54708;
  --info-bg:#eff8ff; --info-line:#2e90fa; --info-ink:#175cd3;
  --shadow: 0 1px 3px rgba(16,24,40,.08), 0 1px 2px rgba(16,24,40,.04);
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg:#0f141b; --panel:#161d27; --ink:#e7edf5; --muted:#9aa7b8;
    --line:#28323f; --accent:#5b8cff; --accent-ink:#8fb0ff;
    --ok-bg:#0d2018; --ok-line:#12b76a; --ok-ink:#6ce9a6;
    --err-bg:#2a1513; --err-line:#f97066; --err-ink:#fda29b;
    --warn-bg:#241a0b; --warn-line:#fdb022; --warn-ink:#fec84b;
    --info-bg:#0e1e2e; --info-line:#53b1fd; --info-ink:#84caff;
    --shadow: 0 1px 3px rgba(0,0,0,.4);
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--bg); color: var(--ink);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.wrap { max-width: 760px; margin: 0 auto; padding: 32px 20px 56px; }
header { text-align: center; margin-bottom: 24px; }
h1 { font-size: 30px; margin: 0 0 8px; letter-spacing: -.02em; }
.tagline { color: var(--muted); margin: 0 auto 14px; max-width: 560px; font-size: 16px; }
.privacy {
  display: inline-block; margin: 0; font-size: 13.5px; color: var(--ok-ink);
  background: var(--ok-bg); border: 1px solid var(--ok-line); border-radius: 999px; padding: 5px 14px;
}
#dropzone {
  background: var(--panel); border: 2px dashed var(--line); border-radius: 16px;
  padding: 40px 20px; text-align: center; cursor: pointer; transition: all .15s ease;
  box-shadow: var(--shadow);
}
#dropzone:hover, #dropzone:focus { border-color: var(--accent); outline: none; }
#dropzone.over { border-color: var(--accent); background: var(--info-bg); transform: translateY(-1px); }
.dz-icon { font-size: 34px; margin-bottom: 10px; }
.dz-main { font-size: 19px; font-weight: 650; }
.dz-sub { color: var(--muted); font-size: 14px; margin-top: 4px; }
.link { color: var(--accent-ink); text-decoration: underline; font-weight: 600; }
#intro { margin-top: 28px; }
#intro h2 { font-size: 16px; margin: 0 0 10px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
.checklist { margin: 0; padding: 0; list-style: none; display: grid; gap: 8px; }
.checklist li { position: relative; padding-left: 28px; color: var(--ink); }
.checklist li::before {
  content: "✓"; position: absolute; left: 0; top: 0; color: var(--ok-ink);
  font-weight: 800;
}
.note { color: var(--muted); font-size: 14px; margin-top: 16px; }
.working { text-align: center; color: var(--muted); padding: 30px; font-size: 16px; }
.result { margin-top: 22px; }
.filename {
  font-weight: 650; font-size: 15px; margin-bottom: 10px; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap;
}
.banner { border-radius: 12px; padding: 16px 18px; border: 1px solid; }
.banner-big { font-size: 18px; font-weight: 700; }
.banner-small { font-size: 14px; margin-top: 2px; opacity: .9; }
.b-ok   { background: var(--ok-bg);   border-color: var(--ok-line);   color: var(--ok-ink); }
.b-error{ background: var(--err-bg);  border-color: var(--err-line);  color: var(--err-ink); }
.b-warn { background: var(--warn-bg); border-color: var(--warn-line); color: var(--warn-ink); }
.b-info { background: var(--info-bg); border-color: var(--info-line); color: var(--info-ink); }
.issues { margin-top: 14px; display: grid; gap: 12px; }
.issue {
  display: flex; gap: 13px; background: var(--panel); border: 1px solid var(--line);
  border-left-width: 5px; border-radius: 10px; padding: 14px 16px; box-shadow: var(--shadow);
}
.issue.error   { border-left-color: var(--err-line); }
.issue.warning { border-left-color: var(--warn-line); }
.issue.info    { border-left-color: var(--info-line); }
.issue-badge {
  flex: 0 0 24px; height: 24px; border-radius: 50%; color: #fff; font-weight: 800;
  font-size: 14px; display: flex; align-items: center; justify-content: center; margin-top: 1px;
}
.issue.error   .issue-badge { background: var(--err-line); }
.issue.warning .issue-badge { background: var(--warn-line); }
.issue.info    .issue-badge { background: var(--info-line); }
.issue-title { font-weight: 650; }
.issue-detail { color: var(--muted); font-size: 14.5px; margin-top: 3px; }
.again { margin-top: 24px; text-align: center; }
#againBtn {
  background: var(--accent); color: #fff; border: none; border-radius: 10px;
  padding: 11px 22px; font-size: 15px; font-weight: 600; cursor: pointer;
}
#againBtn:hover { filter: brightness(1.05); }
footer { margin-top: 40px; text-align: center; color: var(--muted); font-size: 12.5px; }
`;

const HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>CEAB Data Checker</title>
<style>${CSS}</style>
</head>
<body>
${BODY}
<script>
${core}
${UI_JS}
</script>
</body>
</html>
`;

writeFileSync(new URL("../ceab_data_checker.html", import.meta.url), HTML);
console.log("Wrote ceab_data_checker.html (" + HTML.length + " bytes)");
