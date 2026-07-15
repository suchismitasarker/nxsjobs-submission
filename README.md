
# NxRefine Job Submission — CLASSE Cluster (lnx201)

A web-based Flask app for generating and submitting **nxreduce** cluster jobs
at the Quantum Materials (QM2) Beamline, CHESS · Cornell University.

A stand-alone Flask web application for generating and submitting **nxreduce**
data-reduction jobs to the CLASSE compute cluster (SGE / `qsub`), developed for
the **Quantum Materials beamline (QM², CHESS ID4B)**.

It gives beamline users a browser-based interface to turn raw `.nxs` scan
directories into ready-to-run reduction scripts, submit them to the cluster,
and watch their progress — without hand-writing shell scripts or memorizing
`qsub` incantations.

---

## Features

- Automated generation of job submission scripts  
- Browse NFS directories and select sample folders
- Configure temperatures (single, range, or list)
- Select processing steps (Load, Link, Find, Refine, Transform, PDF)
- Generate and submit SGE qsub scripts
- Job history with stdout/stderr viewer
- Handles three common workflows:
  - Running with an **existing parent file**
  - Running when **no parent file is available**
  - Running **additional jobs after a parent file is created**
- Web UI for selecting images, folders, PONI files, masks, and output paths
- Integrated PyFAI support
- Caching and fast slice-viewing for Q-space visualization
- Compatible with SGE cluster environment
- Light/dark theme

- - **Directory browser** — navigate the experiment tree (default root
  `/nfs/chess/id4baux/2026-1`) and pick a user/sample directory.
- **Script generation** — build `nxreduce` shell scripts for selectable stages
  (link, copy, max, find, refine, transform, combine, pdf, …), for a single
  temperature or a range/list of temperatures.
- **Live preview** — see the generated script and the exact `qsub` command
  before anything is submitted.
- **One-click submission** — optionally submit generated scripts to the cluster
  via `qsub` (queue, memory, parallel environment, and cores are configurable).
- **Auto-scan mode** — scan a user directory, build one script per temperature,
  and save (and optionally submit) each in a batch, with a CSV manifest of
  `Temp, Script file, qsub command`.
- **Stage detection** — inspect existing `.nxs` files with h5py to detect which
  reduction stages are already complete, so only the remaining steps are queued.
- **Auto-Watch** — poll a directory for newly completed scans and auto-process
  each temperature once per stage, with an activity log.
- **Job history & queue view** — in-memory history of submitted jobs and a
  `qstat` view of the cluster queue.
- **Light/dark theme** and CHESS branding.

## Requirements

- Python 3.8+
- Python packages: `flask`, `h5py`
- A CLASSE cluster environment with the **SGE** tools (`qsub`, `qstat`) on
  `PATH` and the `nxreduce` command available on the compute nodes. These are
  provided by the beamline/CLASSE environment (e.g. on `lnx201`); the app itself
  only *calls* them.


Install the Python dependencies with:

```bash
pip install flask h5py
```


## How to use the script?
```bash
- ssh <username>@lnx201.classe.cornell.edu
- source /nfs/chess/sw/qm2-data-analysis/bin/activate
```

## Usage
```bash
python nxjobs_app.py
```


Then open:

```
http://<host>:5050
```

The server binds to `0.0.0.0:5050`, so on a beamline workstation it is reachable
from other machines on the same trusted network. From a laptop, prefer SSH port
forwarding:

```bash
ssh -L 5050:localhost:5050 you@lnx201.classe.cornell.edu
# run the app on the remote host, then open http://localhost:5050 locally
```

## How it works

- **Single-file app** — all Python logic and the full HTML/CSS/JS front end live
  in `nxsjobs-app.py`; the UI is served from Flask via `render_template_string`.
- **Script building** — `_build_script()` assembles `nxreduce --directory …`
  command lines for the selected stages; `_save_one_script()` writes them to a
  `scripts/` directory next to the user data (falling back to
  `/tmp/nxreduce_scripts`).
- **Submission** — a `qsub -q <queue> -l mem_free=<mem> -pe <pe> <cores>
  <script>` command is constructed and, when requested, run with
  `subprocess.run(...)`.
- **Stage detection** — `_detect_completed_stages()` opens `.nxs` files with
  h5py and checks for stage markers; short-lived in-memory caches (`_CACHE_TTL`)
  avoid repeating expensive filesystem/h5py work on every refresh.
- **State** — job history, auto-watch bookkeeping, and caches are held in memory
  and cleared on restart; there is no database.

## Routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET / POST | Main UI / generate & submit jobs |
| `/browse_dir` | GET | Directory browser |
| `/scan_temps` | GET | Discover temperatures in a user directory |
| `/watch` | GET | Watcher refresh (stage/scan status, `qstat`) |
| `/autowatch_poll` | POST | Auto-process newly completed scans |
| `/autowatch_reset` | POST | Reset auto-watch state |
| `/clear_history` | POST | Clear in-memory job history |
| `/logo` | GET | Serve the CHESS logo |

## Security notes

- The app binds to `0.0.0.0` and has no authentication. Run it only on a trusted
  beamline/CLASSE network, or reach it via SSH port forwarding as shown above.
- It executes cluster commands (`qsub`, `qstat`, `chmod`) on behalf of the user;
  it should be run as the beamline/experiment user with the appropriate cluster
  permissions.

## Project layout

```
nxsjob-submission/
├── nxsjobs-app.py     # the Flask application (single file)
├── README.md
├── requirements.txt
├── LICENSE
├── CHANGELOG.md
└── .gitignore
```

## Author

Developed by QM2 Beamline Scientist  
CHESS · Cornell University  
Quantum Materials (QM2) Beamline







