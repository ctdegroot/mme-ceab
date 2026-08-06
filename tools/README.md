# CEAB Data Checker (browser tool)

A self-service validator that instructors can run on their own CEAB spreadsheet
**before** submitting it. It reports problems (missing dates, invalid
attributes, text typed into score cells, scores above the maximum, left-over
columns from a previous year, etc.) in plain, non-technical language with
step-by-step fix instructions.

The goal is to stop the back-and-forth over bad data: people fix their own
files first.

## For instructors — how to use it

1. Open **`ceab_data_checker.html`** in a web browser (double-click the file, or
   open the link if it has been posted on SharePoint/Teams).
2. Drag the CEAB spreadsheet onto the page (or click to choose it).
3. Read the results and fix anything marked with a red ✕. Re-check as many times
   as you like.

**No installation. Nothing to set up.** The file is checked entirely inside the
browser — **the spreadsheet never leaves the computer and is never uploaded**,
which matters because these files contain student grades.

Works in any current version of Chrome, Edge, Firefox, or Safari.

## How to distribute it

Pick whichever suits your group — the tool is a single file with no
dependencies:

- **Email** `ceab_data_checker.html` to instructors, or drop it in a shared
  folder. They double-click to open it. Works offline.
- **Host it** on SharePoint/Teams/any web space and share the link, so everyone
  opens the same up-to-date page.

## What it checks

It validates **every user-inputtable field** across all four tabs. The rules are
kept identical to [`scripts/review_ingest.py`](../scripts/review_ingest.py) (the
two are cross-tested to produce the same findings), so a file that passes here
will ingest cleanly. Checks include:

- **Instructor tab** — instructor ID, first name, last name present
- **Course tab** — prefix, number, suffix, academic-year format, and
  year-in-program are valid and complete
- **Measurement tab** — attribute, indicator, deliverable type, deliverable name;
  a real date on every row; a valid grade scale with the downstream values that
  scale needs (`maxScore` for standard bins; `maxScore` + three ordered
  `minPercentScore` thresholds for custom bins)
- **Data tab** — student IDs are usernames (not 9-digit student numbers) and not
  duplicated; no text in score cells; nothing above the maximum or outside 1–4;
  **zeros flagged** so the instructor can confirm each was a real zero rather
  than a "not completed"; no stray data outside the labelled columns; every score
  column matches a measurement (catches left-over columns from a previous year)
- **Template only** — if no scores have been entered yet, it just says so

A note on **zeros**: a blank cell is dropped from the results, but a `0` is
converted to the lowest score (1) and counted — so entering `0` for a
not-completed assessment silently contaminates the data. That's why the tool asks
the instructor to confirm every zero.

## Maintaining it (developers)

The distributable `ceab_data_checker.html` is **generated** — don't hand-edit it.

- `src/validator_core.mjs` — the parser + validation rules (no dependencies;
  uses the browser's built-in `DecompressionStream` to read `.xlsx`). Runs in
  both Node and the browser, so it can be unit-tested from the command line.
- `src/build_html.mjs` — inlines the core into the HTML page. Rebuild with:

  ```bash
  node src/build_html.mjs
  ```

### Keep the rules in sync

Two files hold the same rules and must be changed together: the browser core
`src/validator_core.mjs` and the batch reviewer `scripts/review_ingest.py`. The
valid-value lists at the top of each (`VALID_PREFIXES`, `VALID_SUFFIXES`,
`VALID_ATTRIBUTES`, `VALID_DELIVERABLE_TYPES`, `VALID_GRADE_SCALES`,
`KNOWN_IMPROVEMENT_THEMES`) mirror the `CheckConstraint`s in
[`ceab/models.py`](../ceab/models.py). They are intentionally **broader than the
spreadsheet's own drop-downs**, which are out of date (they omit the
CHEM/NMM/PHYS/STATS/WRIT prefixes and the "Course Grade" deliverable type).

If a rule changes: update both files, re-run the cross-check to confirm they
still agree, then rebuild the HTML (`node src/build_html.mjs`). The two are kept
identical — a change to one without the other will make the browser tool and the
server-side review disagree.
