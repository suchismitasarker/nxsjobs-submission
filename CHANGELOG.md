# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-07-15

### Added
- Initial release of the NxReduce Job Submission App.
- Browser-based UI (Flask) for generating and submitting `nxreduce` cluster jobs.
- Directory browser rooted at the experiment tree.
- Script generation for selectable reduction stages, single or multiple temperatures.
- Live preview of generated scripts and the exact `qsub` command.
- One-click submission via `qsub` with configurable queue, memory, parallel
  environment, and cores.
- Auto-scan mode: one script per temperature, batch save/submit, and a CSV manifest.
- Stage detection of existing `.nxs` files via h5py, with short-lived caching.
- Auto-Watch polling to auto-process newly completed scans, with an activity log.
- In-memory job history and a `qstat` queue view.
- Light/dark theme and CHESS branding.
