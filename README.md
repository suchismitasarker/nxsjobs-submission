
# NxRefine Job Submission — CLASSE Cluster (lnx201)

A web-based Flask app for generating and submitting **nxreduce** cluster jobs
at the Quantum Materials (QM2) Beamline, CHESS · Cornell University.

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

## Requirements

- Python 3.8+
- Flask

#How to use the script?
```bash
- ssh <username>@lnx201.classe.cornell.edu
- source /nfs/chess/sw/qm2-data-analysis/bin/activate
```

## Usage
```bash
python nxjobs_app.py
```

Then open your browser at `http://localhost:5050`

> **Note:** Must be run on a machine with access to the CLASSE SGE cluster
> and the NFS path `/nfs/chess/id4baux/`.


## Author

Developed by QM2 Beamline Scientist  
CHESS · Cornell University  
Quantum Materials (QM2) Beamline







