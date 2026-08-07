// CEAB spreadsheet validator — dependency-free core.
// Parses an .xlsx (a ZIP of XML) using only built-in browser/Node APIs
// (DecompressionStream) and checks every user-inputtable field in a single
// course workbook, with plain-language, non-technical guidance.
//
// The checks here are kept in lock-step with scripts/review_ingest.py — if you
// add or change a rule in one, change the other too.
//
// One string-based parser is used in BOTH Node and the browser so that what we
// test is exactly what ships.
//
// Exposed: parseXlsx(arrayBuffer) -> {sheets}, validate(arrayBuffer) -> report

// ----- valid values (mirror ceab/models.py CheckConstraints) -----
// NB: these are intentionally broader than the spreadsheet's own drop-down
// lists, which are out of date (they omit CHEM/NMM/PHYS/STATS/WRIT prefixes and
// the "Course Grade" deliverable type, all of which are valid and in use).
export const VALID_PREFIXES = ["MME","ECE","ES","ELI","MSE","CHEM","PHYS","STATS","WRIT","NMM"];
export const VALID_SUFFIXES = ["A","B","F","none"];
export const VALID_ATTRIBUTES = ["KB","PA","I","DES","ITW","ET","CS","PR","IES","EE","EPM","LL"];
export const VALID_DELIVERABLE_TYPES = ["Assignment","Final Exam","Lab","Midterm Exam","Presentation","Project","Quiz","Test","Course Grade"];
export const VALID_GRADE_SCALES = ["CEAB (1-4)","Raw Scores (Standard Bins)","Raw Scores (Custom Bins)"];
export const KNOWN_IMPROVEMENT_THEMES = ["GD&T","Computational Tools","Communication","Manufacturing"];
const PREFIX_SET = new Set(VALID_PREFIXES);
const SUFFIX_SET = new Set(VALID_SUFFIXES);
const ATTR_SET = new Set(VALID_ATTRIBUTES);
const DELIV_SET = new Set(VALID_DELIVERABLE_TYPES);
const SCALE_SET = new Set(VALID_GRADE_SCALES);
const THEME_SET = new Set(KNOWN_IMPROVEMENT_THEMES);

export const SHEETS = {
  instructor: "1 - Instructor",
  course: "2 - Course",
  measurement: "3 - Measurement",
  data: "4 - Data",
};

// Levels: "error" = must fix before submitting; "warning" = please double-check;
// "info" = not a problem, just letting you know.
const ERROR = "error", WARNING = "warning", INFO = "info";

// ----- tiny ZIP reader (native inflate, no libraries) -----
async function inflateRaw(bytes) {
  const ds = new DecompressionStream("deflate-raw");
  const stream = new Blob([bytes]).stream().pipeThrough(ds);
  const buf = await new Response(stream).arrayBuffer();
  return new Uint8Array(buf);
}

async function unzip(arrayBuffer) {
  const bytes = new Uint8Array(arrayBuffer);
  const dv = new DataView(arrayBuffer);
  let eocd = -1;
  for (let i = bytes.length - 22; i >= 0; i--) {
    if (dv.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error("NOT_XLSX");
  const cdCount = dv.getUint16(eocd + 10, true);
  let ptr = dv.getUint32(eocd + 16, true);

  const decoder = new TextDecoder("utf-8");
  const entries = [];
  for (let n = 0; n < cdCount; n++) {
    if (dv.getUint32(ptr, true) !== 0x02014b50) break;
    const method = dv.getUint16(ptr + 10, true);
    const compSize = dv.getUint32(ptr + 20, true);
    const nameLen = dv.getUint16(ptr + 28, true);
    const extraLen = dv.getUint16(ptr + 30, true);
    const commentLen = dv.getUint16(ptr + 32, true);
    const localOff = dv.getUint32(ptr + 42, true);
    const name = decoder.decode(bytes.subarray(ptr + 46, ptr + 46 + nameLen));
    const lNameLen = dv.getUint16(localOff + 26, true);
    const lExtraLen = dv.getUint16(localOff + 28, true);
    const dataStart = localOff + 30 + lNameLen + lExtraLen;
    entries.push({ name, method, comp: bytes.subarray(dataStart, dataStart + compSize) });
    ptr += 46 + nameLen + extraLen + commentLen;
  }

  const out = {};
  for (const { name, method, comp } of entries) {
    if (method === 0) out[name] = comp;
    else if (method === 8) out[name] = await inflateRaw(comp);
  }
  return out;
}

// ----- XML helpers -----
const utf8 = (bytes) => new TextDecoder("utf-8").decode(bytes);

function unescapeXml(s) {
  return s.replace(/&lt;/g, "<").replace(/&gt;/g, ">")
          .replace(/&quot;/g, '"').replace(/&apos;/g, "'")
          .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(+d))
          .replace(/&amp;/g, "&");
}

function colToIndex(ref) {
  let col = 0;
  for (const ch of ref) {
    const c = ch.charCodeAt(0);
    if (c >= 65 && c <= 90) col = col * 26 + (c - 64);
    else break;
  }
  return col - 1;
}

export function colLetter(index) {
  let s = "", i = index + 1;
  while (i > 0) { const m = (i - 1) % 26; s = String.fromCharCode(65 + m) + s; i = Math.floor((i - 1) / 26); }
  return s;
}

function parseSharedStrings(xml) {
  if (!xml) return [];
  const strings = [];
  const chunks = xml.split(/<si[ >]/).slice(1);
  for (const chunk of chunks) {
    let s = "";
    const re = /<t[^>]*>([\s\S]*?)<\/t>/g;
    let m;
    while ((m = re.exec(chunk)) !== null) s += m[1];
    strings.push(unescapeXml(s));
  }
  return strings;
}

function cellValue(t, raw, shared) {
  // An empty <v></v> (common in formula cells Excel hasn't recalculated) must be
  // treated as blank, NOT as 0 — Number("") is 0, which would make empty cells
  // look filled.
  if (raw === null || raw === undefined || raw === "") return null;
  if (t === "s") return shared[parseInt(raw, 10)] ?? null;
  if (t === "str") return unescapeXml(raw);
  if (t === "b") return raw === "1";
  const num = Number(raw);
  return Number.isNaN(num) ? unescapeXml(raw) : num;
}

function parseSheetGrid(xml, shared) {
  const grid = [];
  // Match a <row>...</row>, OR a self-closing <row .../>. The lazy attr group
  // plus the (?:/>|>...</row>) alternation prevents a self-closing tag from
  // swallowing following content.
  const rowRe = /<row\b([^>]*?)(?:\/>|>([\s\S]*?)<\/row>)/g;
  let rm;
  while ((rm = rowRe.exec(xml)) !== null) {
    const inner = rm[2];
    const rowArr = [];
    if (inner) {
      const cellRe = /<c\b([^>]*?)(?:\/>|>([\s\S]*?)<\/c>)/g;
      let cm;
      while ((cm = cellRe.exec(inner)) !== null) {
        const attrs = cm[1] || "";
        const body = cm[2];
        const refM = /r="([A-Z0-9]+)"/.exec(attrs);
        const tM = /t="([^"]+)"/.exec(attrs);
        const t = tM ? tM[1] : null;
        const col = refM ? colToIndex(refM[1]) : -1;
        let value = null;
        if (body !== undefined && body !== "") {
          if (t === "inlineStr") {
            const im = /<t[^>]*>([\s\S]*?)<\/t>/.exec(body);
            value = im ? unescapeXml(im[1]) : null;
          } else {
            const vm = /<v[^>]*>([\s\S]*?)<\/v>/.exec(body);
            value = cellValue(t, vm ? vm[1] : null, shared);
          }
        }
        if (col >= 0) rowArr[col] = value;
      }
    }
    grid.push(rowArr);
  }
  return grid;
}

function mapSheetNames(files) {
  const wb = utf8(files["xl/workbook.xml"] || new Uint8Array());
  const rels = utf8(files["xl/_rels/workbook.xml.rels"] || new Uint8Array());
  const relMap = {};
  // Attributes can appear in any order (Excel, openpyxl, LibreOffice all differ),
  // so grab each <Relationship .../> tag and pull Id/Target independently.
  const relRe = /<Relationship\b([^>]*?)\/?>/g;
  let rm;
  while ((rm = relRe.exec(rels)) !== null) {
    const attrs = rm[1];
    const id = /\bId="([^"]+)"/.exec(attrs);
    const tgt = /\bTarget="([^"]+)"/.exec(attrs);
    if (!id || !tgt) continue;
    let target = tgt[1];
    if (target.startsWith("/")) target = target.replace(/^\//, "");
    else if (!target.startsWith("xl/")) target = "xl/" + target;
    relMap[id[1]] = target;
  }
  const nameToPath = {};
  const sheetRe = /<sheet\b([^>]*?)\/?>/g;
  let sm;
  while ((sm = sheetRe.exec(wb)) !== null) {
    const attrs = sm[1];
    const nameM = /name="([^"]*)"/.exec(attrs);
    const ridM = /r:id="([^"]+)"/.exec(attrs);
    if (nameM && ridM && relMap[ridM[1]]) nameToPath[unescapeXml(nameM[1])] = relMap[ridM[1]];
  }
  return nameToPath;
}

export async function parseXlsx(arrayBuffer) {
  const files = await unzip(arrayBuffer);
  const shared = parseSharedStrings(files["xl/sharedStrings.xml"] ? utf8(files["xl/sharedStrings.xml"]) : "");
  const nameToPath = mapSheetNames(files);
  const sheets = {};
  for (const [name, path] of Object.entries(nameToPath)) {
    const xmlBytes = files[path];
    sheets[name] = xmlBytes ? parseSheetGrid(utf8(xmlBytes), shared) : [];
  }
  return { sheets };
}

// ----- value helpers -----
function isBlank(v) {
  return v === null || v === undefined || (typeof v === "string" && v.trim() === "");
}
function txt(v) { return isBlank(v) ? "" : String(v).trim(); }
function num(v) { return typeof v === "number" ? v : Number(v); }
function isNum(v) { return !isBlank(v) && !Number.isNaN(num(v)); }
function headerMap(row) {
  const map = {};
  (row || []).forEach((v, i) => { if (!isBlank(v)) map[String(v).trim()] = i; });
  return map;
}
function joinList(items) {
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")} and ${items[items.length - 1]}`;
}
function capList(items, n) {
  return items.length > n ? `${items.slice(0, n).join(", ")} and ${items.length - n} more` : joinList(items);
}
function niceNum(n) { return Number.isInteger(n) ? String(n) : String(Math.round(n * 10000) / 10000); }
function where(deliverable) { return deliverable ? ` (“${deliverable}”)` : ""; }

// Excel date serial -> calendar year (used only for a plausibility check).
function serialYear(serial) {
  const ms = Math.round((serial - 25569) * 86400000); // Excel epoch 1899-12-30
  return new Date(ms).getUTCFullYear();
}

// ----- validation -----
export function validateGrids(sheets) {
  const issues = [];
  const add = (level, category, title, detail) => issues.push({ level, category, title, detail });

  const missingTabs = [SHEETS.instructor, SHEETS.course, SHEETS.measurement, SHEETS.data]
    .filter((t) => !(t in sheets));
  if (missingTabs.length) {
    add(ERROR, "missing_sheet",
      `The spreadsheet is missing the tab${missingTabs.length > 1 ? "s" : ""}: ${missingTabs.map((t) => `“${t}”`).join(", ")}`,
      "This doesn’t look like the official CEAB template. Please start from the blank CEAB template — it has four tabs named “1 - Instructor”, “2 - Course”, “3 - Measurement”, and “4 - Data” — copy your data into it, and try again.");
  }
  const measGrid = sheets[SHEETS.measurement];
  const dataGrid = sheets[SHEETS.data];
  if (!measGrid || !dataGrid) return finalize(issues);

  // Is the data tab empty (just the template)? If so, say only that.
  const dHead0 = (dataGrid[1] || []).map((v) => (isBlank(v) ? null : String(v).trim()));
  const studentCol0 = dHead0.indexOf("studentID");
  let anyData = false;
  if (studentCol0 >= 0) {
    for (let r = 2; r < dataGrid.length && !anyData; r++) {
      const row = dataGrid[r] || [];
      for (let c = 0; c < row.length; c++) {
        if (c !== studentCol0 && !isBlank(row[c])) { anyData = true; break; }
      }
    }
  }
  if (studentCol0 >= 0 && !anyData) {
    add(INFO, "no_data",
      "No student scores have been entered yet",
      "The “4 - Data” tab only contains the template headings — there are no marks in it. This is fine if you haven’t collected the data yet. Once you’ve entered student scores, run this check again.");
    return finalize(issues);
  }

  checkInstructor(sheets[SHEETS.instructor], add);
  checkCourse(sheets[SHEETS.course], add);
  const measRows = checkMeasurement(measGrid, add);

  // If the measurements have real content but every auto-generated measurementID
  // is blank, Excel hasn't recalculated the formula columns (the Data-tab
  // headings will be blank too). Say so once, instead of a cascade of
  // "unmatched column" / "stray data" errors that all stem from the same cause.
  const contentful = measRows.filter((m) => !isBlank(m.deliverableName) && !isBlank(m.attribute));
  const notRecalculated = contentful.length > 0 && measRows.every((m) => isBlank(m.measurementID));
  if (notRecalculated) {
    add(ERROR, "formulas_not_calculated",
      "This file’s automatic ID columns are blank — it needs to be reopened in Excel",
      "The measurement ID column and the score-column headings on the “4 - Data” tab are filled in automatically by Excel formulas, but they’re currently blank — this usually means the file was saved before Excel recalculated. Everything you typed in looks fine. Please open the file in Excel, press F9 to recalculate (or just save it again), and then re-check it here. This matters for submitting too: the data can’t be processed until those automatic columns fill in.");
  } else {
    checkData(dataGrid, measRows, add);
  }

  return finalize(issues);
}

function checkInstructor(grid, add) {
  if (!grid) return;
  const head = headerMap(grid[0]);
  const get = (row, name) => (name in head ? row[head[name]] : null);
  let any = false;
  for (let r = 1; r < grid.length; r++) {
    const row = grid[r];
    if (!row || row.every(isBlank)) continue;
    any = true;
    // instructorID is auto-generated from the name by a formula, so we only check
    // the fields the instructor actually types.
    const miss = [["firstName", "first name"], ["lastName", "last name"]]
      .filter(([f]) => isBlank(get(row, f))).map(([, label]) => label);
    if (miss.length) {
      add(ERROR, "missing_instructor",
        `On the “1 - Instructor” tab, row ${r + 1} is missing ${joinList(miss)}`,
        "Please fill in the instructor’s first name and last name.");
    }
  }
  if (!any) {
    add(ERROR, "missing_instructor",
      "The “1 - Instructor” tab has no instructor filled in",
      "Please enter the instructor’s ID, first name, and last name on the “1 - Instructor” tab.");
  }
}

function checkCourse(grid, add) {
  if (!grid) return;
  const head = headerMap(grid[0]);
  const get = (row, name) => (name in head ? row[head[name]] : null);
  let any = false;
  for (let r = 1; r < grid.length; r++) {
    const row = grid[r];
    if (!row || row.every(isBlank)) continue;
    any = true;
    const at = `On the “2 - Course” tab, row ${r + 1}`;

    // courseID and instructorID are auto-generated by formulas from the fields
    // below, so we validate those inputs rather than the generated columns.
    const prefix = get(row, "prefix");
    if (isBlank(prefix) || !PREFIX_SET.has(txt(prefix))) {
      add(ERROR, "invalid_prefix",
        `${at}: the course prefix ${isBlank(prefix) ? "is blank" : `“${txt(prefix)}” is not recognised`}`,
        `Use one of the valid subject codes: ${VALID_PREFIXES.join(", ")}.`);
    }
    const number = get(row, "number");
    if (isBlank(number) || !Number.isInteger(num(number)) || num(number) < 1000 || num(number) > 9999) {
      add(ERROR, "invalid_number",
        `${at}: the course number ${isBlank(number) ? "is blank" : `“${txt(number)}” is not valid`}`,
        "The course number must be a 4-digit whole number between 1000 and 9999.");
    }
    const suffix = get(row, "suffix");
    if (isBlank(suffix) || !SUFFIX_SET.has(txt(suffix))) {
      add(ERROR, "invalid_suffix",
        `${at}: the course suffix ${isBlank(suffix) ? "is blank" : `“${txt(suffix)}” is not valid`}`,
        "The suffix must be A, B, F, or none (type the word “none” if the course has no letter).");
    }
    const year = get(row, "academicYear");
    if (isBlank(year) || !/^\d{4}\/\d{2}$/.test(txt(year))) {
      add(ERROR, "invalid_academic_year",
        `${at}: the academic year ${isBlank(year) ? "is blank" : `“${txt(year)}” is not in the right format`}`,
        "Write the academic year as four digits, a slash, then two digits — for example 2025/26.");
    }
    const yip = get(row, "yearInProgram");
    if (isBlank(yip) || !Number.isInteger(num(yip)) || num(yip) < 1 || num(yip) > 4) {
      add(ERROR, "invalid_year_in_program",
        `${at}: the year in program ${isBlank(yip) ? "is blank" : `“${txt(yip)}” is not valid`}`,
        "The year in program must be a whole number from 1 to 4.");
    }
  }
  if (!any) {
    add(ERROR, "missing_course_field",
      "The “2 - Course” tab has no course filled in",
      "Please fill in the course details on the “2 - Course” tab.");
  }
}

function checkMeasurement(grid, add) {
  const head = headerMap(grid[0]);
  const mCol = (name) => (name in head ? head[name] : -1);
  const dateColLetter = mCol("date") >= 0 ? colLetter(mCol("date")) : "the date";
  const maxColLetter = mCol("maxScore") >= 0 ? colLetter(mCol("maxScore")) : "the maxScore";

  const rows = [];
  const missingDateRows = [];
  for (let r = 1; r < grid.length; r++) {
    const row = grid[r];
    if (!row || row.every(isBlank)) continue;
    const get = (name) => { const c = mCol(name); return c >= 0 ? row[c] : null; };
    const m = {
      excelRow: r + 1,
      measurementID: get("measurementID"),
      attribute: get("attribute"),
      indicator: get("indicator"),
      deliverableType: get("deliverableType"),
      deliverableName: get("deliverableName"),
      date: get("date"),
      gradeScale: get("gradeScale"),
      maxScore: get("maxScore"),
      minPercentScore2: get("minPercentScore2"),
      minPercentScore3: get("minPercentScore3"),
      minPercentScore4: get("minPercentScore4"),
      improvementTheme: get("improvementTheme"),
    };
    rows.push(m);
    const at = `On the “3 - Measurement” tab, row ${m.excelRow}${where(m.deliverableName)}`;

    if (isBlank(m.deliverableName)) {
      add(ERROR, "missing_deliverable_name",
        `On the “3 - Measurement” tab, row ${m.excelRow}: the deliverable name is blank`,
        "Give this measurement a short name (for example, “Final Exam Q4” or “Lab 3”).");
    }
    if (isBlank(m.attribute) || !ATTR_SET.has(txt(m.attribute))) {
      add(ERROR, "invalid_attribute",
        `${at}: the Graduate Attribute ${isBlank(m.attribute) ? "is blank" : `“${txt(m.attribute)}” is not recognised`}`,
        `Choose a valid attribute from the drop-down list. The allowed codes are: ${VALID_ATTRIBUTES.join(", ")}.`);
    }
    if (isBlank(m.indicator) || ![1, 2, 3, 4].includes(num(m.indicator))) {
      add(ERROR, "invalid_indicator",
        `${at}: the Indicator ${isBlank(m.indicator) ? "is blank" : `is “${txt(m.indicator)}”`}`,
        "The indicator must be a whole number from 1 to 4. Please correct this cell.");
    }
    if (isBlank(m.deliverableType) || !DELIV_SET.has(txt(m.deliverableType))) {
      add(ERROR, "invalid_deliverable_type",
        `${at}: the Deliverable Type ${isBlank(m.deliverableType) ? "is blank" : `“${txt(m.deliverableType)}” is not recognised`}`,
        `Choose one from the drop-down list. The allowed types are: ${VALID_DELIVERABLE_TYPES.join(", ")}.`);
    }

    // date: required, and must look like a real date
    if (isBlank(m.date)) {
      missingDateRows.push(String(m.excelRow));
    } else if (typeof m.date !== "number") {
      add(ERROR, "invalid_date_value",
        `${at}: the date “${txt(m.date)}” isn’t a real date`,
        "Type the date into the cell using Excel’s date format (for example 2025-10-07), rather than as plain text.");
    } else {
      const y = serialYear(m.date);
      if (y < 2015 || y > 2035) {
        add(WARNING, "invalid_date_value",
          `${at}: the date doesn’t look right (it reads as the year ${y})`,
          "Please check this cell — it may have been typed as a plain number or a year by mistake. Enter it using Excel’s date format (for example 2025-10-07).");
      }
    }

    // gradeScale + its downstream requirements
    const scale = txt(m.gradeScale);
    if (isBlank(m.gradeScale) || !SCALE_SET.has(scale)) {
      add(ERROR, "invalid_grade_scale",
        `${at}: the grade scale ${isBlank(m.gradeScale) ? "is blank" : `“${txt(m.gradeScale)}” is not recognised`}`,
        `Choose one from the drop-down list: “CEAB (1-4)” if you already scored on the 1–4 scale, “Raw Scores (Standard Bins)” for raw marks, or “Raw Scores (Custom Bins)” for raw marks with your own thresholds.`);
    } else if (scale === "Raw Scores (Standard Bins)") {
      if (isBlank(m.maxScore)) {
        add(ERROR, "missing_maxscore",
          `${at}: the grade scale is “Raw Scores (Standard Bins)” but the maxScore is blank`,
          `Enter the maximum possible mark for this assessment in column ${maxColLetter} (for example, if it was marked out of 30, type 30). The tool needs this to convert raw marks to the 1–4 scale.`);
      } else if (!isNum(m.maxScore) || num(m.maxScore) <= 0) {
        add(ERROR, "invalid_max_score",
          `${at}: the maxScore “${txt(m.maxScore)}” is not a valid number`,
          "The maximum score must be a number greater than 0.");
      }
    } else if (scale === "Raw Scores (Custom Bins)") {
      const fields = [
        ["maxScore", "the maximum score"],
        ["minPercentScore2", "the minimum % for a 2"],
        ["minPercentScore3", "the minimum % for a 3"],
        ["minPercentScore4", "the minimum % for a 4"],
      ];
      const missing = fields.filter(([f]) => isBlank(m[f])).map(([, l]) => l);
      if (missing.length) {
        add(ERROR, "missing_custom_bins",
          `${at}: the grade scale is “Raw Scores (Custom Bins)” but some values are blank`,
          `Please fill in: ${joinList(missing)}. All four values are needed so the tool knows how to turn raw marks into 1–4 scores.`);
      } else {
        if (!isNum(m.maxScore) || num(m.maxScore) <= 0) {
          add(ERROR, "invalid_max_score",
            `${at}: the maxScore “${txt(m.maxScore)}” is not a valid number`,
            "The maximum score must be a number greater than 0.");
        }
        const p2 = num(m.minPercentScore2), p3 = num(m.minPercentScore3), p4 = num(m.minPercentScore4);
        const pcts = [["minimum % for a 2", p2], ["minimum % for a 3", p3], ["minimum % for a 4", p4]];
        const badRange = pcts.filter(([, v]) => Number.isNaN(v) || v < 0 || v > 100).map(([l]) => l);
        if (badRange.length) {
          add(ERROR, "invalid_custom_bins",
            `${at}: some custom-bin percentages are out of range`,
            `${joinList(badRange)} must each be a number between 0 and 100.`);
        } else if (!(p2 < p3 && p3 < p4)) {
          add(ERROR, "invalid_custom_bins",
            `${at}: the custom-bin percentages are not in increasing order`,
            `The thresholds must increase: the minimum % for a 2 (${niceNum(p2)}) must be less than for a 3 (${niceNum(p3)}), which must be less than for a 4 (${niceNum(p4)}).`);
        }
      }
    }

    // improvementTheme (optional) — gentle nudge if it isn't a usual value
    if (!isBlank(m.improvementTheme) && !THEME_SET.has(txt(m.improvementTheme))) {
      add(WARNING, "unusual_improvement_theme",
        `${at}: the improvement theme “${txt(m.improvementTheme)}” isn’t one of the usual options`,
        `This field is optional. If you meant one of the standard themes (${KNOWN_IMPROVEMENT_THEMES.join(", ")}), pick it from the drop-down; otherwise you can ignore this note.`);
    }
  }

  if (missingDateRows.length) {
    add(ERROR, "missing_date",
      `${missingDateRows.length} measurement${missingDateRows.length > 1 ? "s are" : " is"} missing a date`,
      `On the “3 - Measurement” tab, fill in the date (column ${dateColLetter}) for row${missingDateRows.length > 1 ? "s" : ""} ${capList(missingDateRows, 12)}. Use the date the assessment took place. If the assessment took place over multiple dates, use the earliest date.`);
  }
  return rows;
}

function checkData(grid, measRows, add) {
  const dHead = (grid[1] || []).map((v) => (isBlank(v) ? null : String(v).trim()));
  const studentCol = dHead.indexOf("studentID");
  if (studentCol < 0) {
    add(ERROR, "missing_sheet",
      "The “4 - Data” tab doesn’t have a “studentID” heading",
      "On the “4 - Data” tab, the second row should start with a heading called “studentID”, followed by one column for each measurement. Please use the official template so these headings are correct.");
    return;
  }

  const valueCols = [];
  dHead.forEach((name, i) => { if (name && i !== studentCol) valueCols.push({ name, i }); });
  const valueColSet = new Set(valueCols.map((v) => v.i));

  // Scan rows for scores, stray data, orphan rows, and student IDs.
  const scoresByCol = {};           // header name -> array of raw values
  const studentIDs = [];            // [{id, row}]
  const strayCols = new Set();      // column letters with data outside labelled columns
  const orphanRows = [];            // rows with data but no studentID
  for (let r = 2; r < grid.length; r++) {
    const row = grid[r] || [];
    const nonEmpty = [];
    for (let c = 0; c < row.length; c++) if (!isBlank(row[c])) nonEmpty.push(c);
    if (!nonEmpty.length) continue;

    const hasStudent = !isBlank(row[studentCol]);
    if (hasStudent) studentIDs.push({ id: txt(row[studentCol]), row: r + 1 });

    const dataCells = nonEmpty.filter((c) => c !== studentCol);
    if (dataCells.length && !hasStudent) orphanRows.push(String(r + 1));

    for (const c of dataCells) {
      if (valueColSet.has(c)) {
        const name = dHead[c];
        (scoresByCol[name] ||= []).push(row[c]);
      } else {
        strayCols.add(colLetter(c));
      }
    }
  }

  // studentID: must not be a plain number (student number typed by mistake)
  const numericIDs = studentIDs.filter((s) => /^\d+$/.test(s.id));
  if (numericIDs.length) {
    const rowsList = capList(numericIDs.map((s) => String(s.row)), 12);
    add(ERROR, "numeric_student_id",
      `${numericIDs.length} student ID${numericIDs.length > 1 ? "s look" : " looks"} like a student number instead of a username`,
      `On the “4 - Data” tab, the studentID should be the Western login/username (letters and numbers, e.g. “jsmith42”), not the 9-digit student number. Please fix the studentID in row${numericIDs.length > 1 ? "s" : ""} ${rowsList}.`);
  }

  // duplicate studentIDs
  const seen = new Map();
  const dups = new Set();
  for (const s of studentIDs) { if (seen.has(s.id)) dups.add(s.id); else seen.set(s.id, s.row); }
  if (dups.size) {
    add(WARNING, "duplicate_student_id",
      `The same studentID appears more than once`,
      `On the “4 - Data” tab, ${capList([...dups].map((d) => `“${d}”`), 8)} appear${dups.size > 1 ? "" : "s"} on more than one row. Each student should have a single row. Please remove or merge the duplicates.`);
  }

  // stray data outside the labelled columns
  if (strayCols.size) {
    add(ERROR, "stray_data",
      `There is data outside the labelled score columns on the “4 - Data” tab`,
      `Found values in column${strayCols.size > 1 ? "s" : ""} ${capList([...strayCols], 8)}, which don’t have a measurement heading in row 2. Scores must go only in the labelled columns. Please move or clear this stray data (use “Clear Contents”, don’t delete the columns).`);
  }

  // orphan rows (scores with no studentID)
  if (orphanRows.length) {
    add(ERROR, "orphan_row",
      `Some rows have scores but no studentID`,
      `On the “4 - Data” tab, row${orphanRows.length > 1 ? "s" : ""} ${capList(orphanRows, 12)} contain marks but the studentID is blank. Add the missing studentID, or clear the row if it isn’t needed.`);
  }

  const measIDs = new Set(measRows.map((m) => m.measurementID).filter((x) => !isBlank(x)));
  const measByID = {};
  for (const m of measRows) measByID[m.measurementID] = m;

  // columns of scores that don't match a measurement
  for (const name of Object.keys(scoresByCol).sort()) {
    if (!measIDs.has(name)) {
      const oldYear = /20\d\d\/\d\d/.exec(name);
      const hint = oldYear ? ` The “${oldYear[0]}” in the name suggests it’s left over from a previous year.` : "";
      add(ERROR, "unmatched_data_column",
        `On the “4 - Data” tab there is a column of scores that doesn’t match any measurement`,
        `The column labelled “${name}” has marks in it, but there is no matching row on the “3 - Measurement” tab.${hint} Either delete this column, or add the matching measurement on the Measurement tab. Every column of scores must line up with a measurement.`);
    }
  }
  // measurements with no data column
  for (const mid of [...measIDs].sort()) {
    if (!(mid in scoresByCol)) {
      const m = measByID[mid];
      add(WARNING, "measurement_without_data",
        `A measurement on the “3 - Measurement” tab${where(m && m.deliverableName)} has no scores`,
        `There is no column of marks for “${mid}” on the “4 - Data” tab. If you meant to collect data for it, add a column with that heading and enter the scores. If not, you can ignore this.`);
    }
  }

  // per-column: text, range, zeros
  const zeroCols = [];
  let zeroTotal = 0;
  for (const name of Object.keys(scoresByCol).sort()) {
    const raw = scoresByCol[name];
    const numeric = [];
    const badSet = new Set();
    let zeros = 0;
    for (const v of raw) {
      const n = num(v);
      if (Number.isNaN(n)) badSet.add(txt(v));
      else { numeric.push(n); if (n === 0) zeros++; }
    }
    const m = measByID[name];
    const label = m && m.deliverableName ? `“${m.deliverableName}”` : `“${name}”`;

    if (badSet.size) {
      const bad = [...badSet].sort();
      const preview = bad.slice(0, 5).map((b) => `“${b}”`).join(", ");
      const more = bad.length > 5 ? `, and ${bad.length - 5} other value(s)` : "";
      add(ERROR, "non_numeric_score",
        `On the “4 - Data” tab, the ${label} column contains text where a score should be`,
        `Found: ${preview}${more}. Score cells must contain numbers only. If a student didn’t complete the work, leave the cell blank (see the note about zeros) — don’t type words like “absent” or “academic consideration” into the score cells.`);
    }
    if (zeros) { zeroCols.push(label); zeroTotal += zeros; }

    if (!m || !numeric.length) continue;
    const scale = txt(m.gradeScale);
    if (scale === "Raw Scores (Standard Bins)" || scale === "Raw Scores (Custom Bins)") {
      const max = num(m.maxScore);
      if (isNum(m.maxScore) && max > 0) {
        const over = numeric.filter((s) => s > max || s < 0);
        if (over.length) {
          add(ERROR, "score_exceeds_max",
            `On the “4 - Data” tab, the ${label} column has ${over.length} score${over.length > 1 ? "s" : ""} outside the allowed range`,
            `You set the maximum for this assessment to ${niceNum(max)}, but some marks are higher than that or below zero (for example, ${niceNum(Math.max(...over))}). Either the maxScore on the Measurement tab is wrong, or the marks were entered on a different scale (for example as percentages). Please check and correct one of them.`);
        }
      }
    } else if (scale === "CEAB (1-4)") {
      const out = numeric.filter((s) => s !== 0 && (s < 1 || s > 4));
      if (out.length) {
        add(WARNING, "ceab_out_of_range",
          `On the “4 - Data” tab, the ${label} column is set to the “CEAB (1-4)” scale but has scores outside 1–4`,
          `${out.length} mark${out.length > 1 ? "s are" : " is"} outside the 1–4 range (for example, ${niceNum(Math.max(...out))}). If these are raw marks out of a larger total, change the grade scale for this measurement to one of the “Raw Scores” options on the Measurement tab.`);
      }
    }
  }

  // zeros — one combined, prominent note
  if (zeroTotal) {
    add(WARNING, "zeros_present",
      `There ${zeroTotal > 1 ? "are" : "is"} ${zeroTotal} zero${zeroTotal > 1 ? "s" : ""} in the scores — please double-check ${zeroTotal > 1 ? "them" : "it"}`,
      `A blank cell is left out of the results, but a 0 is counted as a completed assessment with the lowest score. If a student did not complete the work, clear the cell (leave it blank) so it isn’t scored against them. Only keep a 0 if the student genuinely attempted it and earned nothing. Zeros were found in: ${capList(zeroCols, 8)}.`);
  }
}

function finalize(issues) {
  const errors = issues.filter((i) => i.level === ERROR).length;
  const warnings = issues.filter((i) => i.level === WARNING).length;
  const infos = issues.filter((i) => i.level === INFO).length;
  return { issues, errors, warnings, infos, ok: errors === 0 && warnings === 0 };
}

export async function validate(arrayBuffer) {
  try {
    const { sheets } = await parseXlsx(arrayBuffer);
    return validateGrids(sheets);
  } catch (e) {
    if (e && e.message === "NOT_XLSX") {
      return finalize([{
        level: ERROR, category: "not_xlsx",
        title: "This file isn’t a readable Excel (.xlsx) workbook",
        detail: "Please make sure you’re uploading the CEAB spreadsheet saved as an Excel Workbook (.xlsx). If it’s an older .xls file, open it in Excel and use “Save As” to save a copy as .xlsx, then try again.",
      }]);
    }
    return finalize([{
      level: ERROR, category: "read_error",
      title: "Sorry — this file couldn’t be read",
      detail: "The spreadsheet couldn’t be opened. Make sure it isn’t password-protected or still open in Excel, then try again.",
    }]);
  }
}
