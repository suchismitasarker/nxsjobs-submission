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





