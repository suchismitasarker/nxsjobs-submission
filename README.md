
# NxRefine Job Submission — CLASSE Cluster (lnx201)

A web-based Flask app for generating and submitting **nxreduce** cluster jobs
at the Quantum Materials (QM2) Beamline, CHESS · Cornell University.

---

## Features

- Browse NFS directories and select sample folders
- Configure temperatures (single, range, or list)
- Select processing steps (Load, Link, Find, Refine, Transform, PDF)
- Generate and submit SGE qsub scripts
- View live queue status (`qstat`) with auto-refresh
- Open SGE output files (`.pe` / `.po`) directly in the browser
- Job history with stdout/stderr viewer
- Light/dark theme

## Requirements

- Python 3.8+
- Flask


## Usage
```bash
python nxjobs_app.py
```

Then open your browser at `http://localhost:5050`

> **Note:** Must be run on a machine with access to the CLASSE SGE cluster
> and the NFS path `/nfs/chess/id4baux/`.

## Configuration

Edit these two lines at the top of `nxjobs_app.py`:
```python
ROOT       = "/nfs/chess/id4baux/2026-1"       # root for directory browser
CHESS_LOGO = "/nfs/chess/id4baux/chesslogo.png" # path to CHESS logo
```

## Author

Developed by QM2 Beamline Scientist  
CHESS · Cornell University  
Quantum Materials (QM2) Beamline



# NxRefine Job Submission Tool

This repository contains a command-line tool and web interface for submitting NxRefine jobs on the CLASSE (lnx201) cluster for the Quantum Materials Beamline at CHESS.

The tool simplifies job creation, handles parent-file logic, and provides a user-friendly workflow for running NxRefine with or without existing parent files.

---

## 🚀 Features

- Automated generation of job submission scripts  
- Handles three common workflows:
  - Running with an **existing parent file**
  - Running when **no parent file is available**
  - Running **additional jobs after a parent file is created**
- Web UI for selecting images, folders, PONI files, masks, and output paths
- Integrated PyFAI support
- Caching and fast slice-viewing for Q-space visualization
- Compatible with SGE cluster environment

---

## 📂 Repository Structure




# nxsjobs-submission
This is the repository of the quantum materials beamline job submission app for the CLASSE cluster 

#How to use the script?
* ssh <username>@lnx201.classe.cornell.edu
* source /nfs/chess/sw/qm2-data-analysis/bin/activate
* python nxsjobs-app.py





