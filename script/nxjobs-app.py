#!/usr/bin/env python3
"""
NxReduce Job Submission App
Stand-alone Flask app for generating and submitting nxreduce cluster jobs.
Usage:  python nxjobs_app.py
"""

from flask import Flask, render_template_string, request, jsonify, send_file, Response
import os, datetime, subprocess, threading

app = Flask(__name__)

# ── Root for directory browser ────────────────────────────────────────────────
ROOT = "/nfs/chess/id4baux/2026-1"

# ── CHESS logo ────────────────────────────────────────────────────────────────
CHESS_LOGO = "/nfs/chess/id4baux/chesslogo.png"

# ── In-memory job history (cleared on restart) ────────────────────────────────
_job_history = []
_history_lock = threading.Lock()

# ── HTML ──────────────────────────────────────────────────────────────────────
PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NxRefine Job Submission CLASSE Cluster for Quantum Materials Beamline(lnx201) · CHESS</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#f0f4f9;--bg-card:#fff;--bg-header:#0d1520;
    --text:#1e293b;--text-muted:#64748b;--text-header:#e2e8f0;
    --border:#d1d9e6;--accent:#0284c7;--accent2:#0ea5e9;
    --green:#16a34a;--green2:#22c55e;--red:#dc2626;
    --gold:#d97706;--mono:'IBM Plex Mono',monospace;
    --sans:'IBM Plex Sans',-apple-system,sans-serif;
  }
  body.dark{
    --bg:#060e1c;--bg-card:#0d1a2e;--bg-header:#020b14;
    --text:#e2e8f0;--text-muted:#94a3b8;--border:#1e3a5f;
    --accent:#38bdf8;--accent2:#0ea5e9;
  }
  *{box-sizing:border-box;margin:0;padding:0;}
  body{font-family:var(--sans);background:var(--bg);color:var(--text);min-height:100vh;}

  /* ── header ── */
  header{
    background:var(--bg-header);color:var(--text-header);
    padding:0 32px;height:56px;display:flex;align-items:center;
    justify-content:space-between;position:sticky;top:0;z-index:100;
    box-shadow:0 2px 16px rgba(0,0,0,0.5);
  }
  .hdr-left{display:flex;align-items:center;gap:14px;}
  .logo{width:150px;height:150px;border-radius:6px;overflow:hidden;
    display:flex;align-items:center;justify-content:center;flex-shrink:0;}
  .logo img{width:150px;height:150px;object-fit:contain;}
  .logo.fallback{border-radius:50%;
    background:conic-gradient(from 0deg,#00c8ff 0%,#0047ab 45%,#00c8ff 100%);
    font-size:12px;font-weight:700;color:#fff;box-shadow:0 0 12px rgba(0,200,255,0.35);}
  .hdr-title{font-size:20px;font-weight:900;letter-spacing:.01em;}
  .hdr-sub{font-size:12px;opacity:.55;margin-top:1px;}
  .badge{display:inline-flex;align-items:center;gap:5px;
    font-size:11px;padding:3px 10px;border-radius:20px;font-weight:600;}
  .badge-blue{background:rgba(56,189,248,.12);color:#38bdf8;border:1px solid rgba(56,189,248,.25);}
  .badge-green{background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25);}
  .badge-red{background:rgba(239,68,68,.12);color:#ef4444;border:1px solid rgba(239,68,68,.25);}
  .badge-gold{background:rgba(251,191,36,.1);color:#fbbf24;border:1px solid rgba(251,191,36,.2);}
  .theme-btn{background:transparent;border:1px solid rgba(255,255,255,.25);color:var(--text-header);
    border-radius:20px;padding:4px 12px;font-size:11px;cursor:pointer;font-family:var(--sans);}
  .theme-btn:hover{background:rgba(255,255,255,.08);}

  /* ── layout ── */
  .page{max-width:1100px;margin:0 auto;padding:28px 24px 48px;}

  /* ── cards ── */
  .card{background:var(--bg-card);border:1px solid var(--border);border-radius:10px;
    padding:20px 24px;margin-bottom:20px;box-shadow:0 1px 4px rgba(15,23,42,.08);}
  .card-header{display:flex;align-items:center;justify-content:space-between;
    padding-bottom:14px;margin-bottom:16px;border-bottom:1px solid var(--border);}
  .card-title{font-size:11px;font-weight:700;letter-spacing:.1em;
    text-transform:uppercase;color:var(--text-muted);display:flex;align-items:center;gap:8px;}
  .dot{width:6px;height:6px;border-radius:50%;background:#38bdf8;
    box-shadow:0 0 8px #38bdf8;flex-shrink:0;}
  .dot-green{background:#22c55e;box-shadow:0 0 8px #22c55e;}
  .dot-red{background:#ef4444;box-shadow:0 0 8px #ef4444;}
  .dot-gold{background:#fbbf24;box-shadow:0 0 8px #fbbf24;}

  /* ── form elements ── */
  label.field-label{display:block;font-size:11px;font-weight:700;
    letter-spacing:.07em;text-transform:uppercase;color:var(--text-muted);margin-bottom:5px;}
  .field{margin-bottom:16px;}
  input[type=text],input[type=number]{
    padding:7px 11px;border:1px solid var(--border);border-radius:6px;
    font-size:13px;font-family:var(--sans);background:var(--bg-card);color:var(--text);
    outline:none;transition:border-color .15s,box-shadow .15s;width:100%;}
  input[type=text]:focus,input[type=number]:focus{
    border-color:var(--accent);box-shadow:0 0 0 3px rgba(2,132,199,.12);}
  input[type=number]{width:110px;}
  input[type=checkbox],input[type=radio]{accent-color:var(--accent);}
  .row{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;}
  .row .field{margin-bottom:0;}

  /* ── step cards ── */
  .steps-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px;}
  .step-card{border:1px solid var(--border);border-radius:8px;padding:10px 14px;
    display:flex;align-items:flex-start;gap:10px;cursor:pointer;
    transition:border-color .15s,background .15s;}
  .step-card:hover{border-color:var(--accent);background:rgba(2,132,199,.04);}
  .step-card input[type=checkbox]{margin-top:3px;flex-shrink:0;}
  .step-name{font-size:13px;font-weight:600;margin-bottom:2px;}
  .step-cmd{font-family:var(--mono);font-size:11px;color:var(--accent);margin-bottom:3px;}
  .step-desc{font-size:11px;color:var(--text-muted);}

  /* ── step presets ── */
  .preset-bar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;}
  .preset-btn{padding:6px 16px;border-radius:20px;border:1px solid var(--border);
    font-size:12px;font-weight:600;cursor:pointer;background:var(--bg-card);color:var(--text-muted);
    font-family:var(--sans);transition:all .15s;white-space:nowrap;}
  .preset-btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(2,132,199,.06);}
  .preset-btn.active{background:rgba(56,189,248,.12);color:#38bdf8;border-color:rgba(56,189,248,.4);}

  /* ── temperature mode toggle ── */
  .temp-modes{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px;}
  .mode-btn{padding:5px 14px;border-radius:20px;border:1px solid var(--border);
    font-size:12px;font-weight:600;cursor:pointer;background:var(--bg-card);color:var(--text-muted);
    font-family:var(--sans);transition:all .15s;}
  .mode-btn.active{background:var(--accent);color:#fff;border-color:var(--accent);}

  /* ── buttons ── */
  .btn{display:inline-flex;align-items:center;gap:7px;padding:9px 20px;
    border-radius:6px;font-size:13px;font-weight:600;font-family:var(--sans);
    cursor:pointer;border:none;transition:opacity .15s,box-shadow .15s;letter-spacing:.02em;}
  .btn:hover{opacity:.88;}
  .btn-primary{background:linear-gradient(135deg,#0369a1,#0ea5e9);color:#fff;
    box-shadow:0 2px 12px rgba(14,165,233,.25);}
  .btn-submit{background:linear-gradient(135deg,#15803d,#22c55e);color:#fff;
    box-shadow:0 2px 12px rgba(34,197,94,.25);}
  .btn-clear{background:rgba(148,163,184,.12);color:var(--text-muted);
    border:1px solid var(--border);}

  /* ── script preview ── */
  .script-pre{background:#060e1c;color:#cbd5e1;padding:16px 18px;border-radius:8px;
    font-family:var(--mono);font-size:12px;line-height:1.7;overflow-x:auto;
    white-space:pre;border:1px solid rgba(56,189,248,.12);max-height:420px;overflow-y:auto;}
  .qsub-bar{padding:10px 14px;background:rgba(56,189,248,.05);
    border:1px solid rgba(56,189,248,.18);border-radius:6px;
    font-family:var(--mono);font-size:12px;margin-top:12px;
    display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
  .qsub-label{font-size:11px;color:var(--text-muted);flex-shrink:0;}

  /* ── job history table ── */
  table{border-collapse:collapse;width:100%;font-size:12px;}
  th{text-align:left;font-size:10px;font-weight:700;letter-spacing:.08em;
    text-transform:uppercase;color:var(--text-muted);padding:7px 12px;
    background:rgba(148,163,184,.08);border-bottom:1px solid var(--border);}
  td{padding:8px 12px;border-bottom:1px solid var(--border);
    font-family:var(--mono);vertical-align:top;}
  tr:last-child td{border-bottom:none;}
  tr:hover td{background:rgba(148,163,184,.04);}
  .job-ok{color:#22c55e;font-weight:600;}
  .job-fail{color:#ef4444;font-weight:600;}

  /* ── alerts ── */
  .alert-error{background:rgba(239,68,68,.06);border:1px solid rgba(239,68,68,.25);
    color:#dc2626;border-radius:6px;padding:10px 14px;font-size:13px;margin-bottom:12px;}
  .alert-success{background:rgba(34,197,94,.06);border:1px solid rgba(34,197,94,.25);
    color:#15803d;border-radius:6px;padding:12px 16px;font-size:14px;font-weight:600;}
  code{font-family:var(--mono);font-size:11px;background:rgba(148,163,184,.12);
    padding:1px 5px;border-radius:3px;}

  /* ── path preview ── */
  .path-preview{display:none;margin-top:8px;padding:8px 12px;
    background:rgba(2,132,199,.06);border:1px solid rgba(2,132,199,.2);
    border-radius:6px;font-size:12px;line-height:1.6;}
  .path-preview .pp-label{color:var(--text-muted);font-size:11px;font-weight:600;
    letter-spacing:.06em;text-transform:uppercase;margin-bottom:3px;}
  .path-preview .pp-base{font-family:var(--mono);color:var(--text);}
  .path-preview .pp-temp{font-family:var(--mono);color:#f59e0b;font-weight:700;}
  .path-preview .pp-full{font-family:var(--mono);font-size:11px;color:var(--text-muted);margin-top:4px;}

  /* ── misc ── */
  .hint{font-size:11px;color:var(--text-muted);margin-top:4px;}
  .divider{height:1px;background:var(--border);margin:20px 0;}
  ::-webkit-scrollbar{width:5px;height:5px;}
  ::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}

  /* ── directory picker modal ── */
  .modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:500;
    display:none;align-items:center;justify-content:center;}
  .modal-overlay.open{display:flex;}
  .modal-box{background:var(--bg-card);border:1px solid var(--border);border-radius:12px;
    width:min(680px,95vw);max-height:80vh;display:flex;flex-direction:column;
    box-shadow:0 20px 60px rgba(0,0,0,.45);}
  .modal-header{padding:16px 20px;border-bottom:1px solid var(--border);
    display:flex;align-items:center;justify-content:space-between;flex-shrink:0;}
  .modal-title{font-size:13px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-muted);}
  .modal-close{background:none;border:none;font-size:20px;cursor:pointer;color:var(--text-muted);padding:0 4px;line-height:1;}
  .modal-close:hover{color:var(--text);}
  .modal-crumbs{padding:10px 20px;border-bottom:1px solid var(--border);
    display:flex;flex-wrap:wrap;gap:4px;align-items:center;flex-shrink:0;font-size:12px;}
  .crumb{color:var(--accent);cursor:pointer;font-family:var(--mono);}
  .crumb:hover{text-decoration:underline;}
  .crumb-sep{color:var(--text-muted);margin:0 2px;}
  .modal-body{flex:1;overflow-y:auto;padding:10px 12px;}
  .dir-item{display:flex;align-items:center;gap:10px;padding:7px 12px;
    border-radius:6px;cursor:pointer;font-size:13px;font-family:var(--mono);}
  .dir-item:hover{background:rgba(2,132,199,.08);color:var(--accent);}
  .modal-footer{padding:12px 20px;border-top:1px solid var(--border);
    display:flex;gap:10px;align-items:center;flex-shrink:0;}
  .modal-cur{font-family:var(--mono);font-size:11px;color:var(--text-muted);
    flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .browse-btn{padding:6px 14px;border-radius:6px;border:1px solid var(--border);
    background:rgba(2,132,199,.08);color:var(--accent);font-size:12px;font-weight:600;
    font-family:var(--sans);cursor:pointer;white-space:nowrap;}
  .browse-btn:hover{background:rgba(2,132,199,.2);}
</style>
<script>
function applyTheme(){if(localStorage.getItem('nxjobs_theme')==='dark')document.body.classList.add('dark');}
function toggleTheme(){document.body.classList.toggle('dark');localStorage.setItem('nxjobs_theme',document.body.classList.contains('dark')?'dark':'light');}

function setTempMode(mode){
  document.querySelectorAll('.mode-btn').forEach(function(b){b.classList.remove('active');});
  document.getElementById('modeBtn_'+mode).classList.add('active');
  document.getElementById('temp_mode_val').value=mode;
  ['single','range','list'].forEach(function(m){
    document.getElementById('tblock_'+m).style.display=(m===mode)?'':'none';
  });
  updatePathPreview();
}

// ── Live path + script-name preview ───────────────────────────────────────────
function _escHtml(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function _getFirstTemp(){
  var mode=document.getElementById('temp_mode_val').value;
  if(mode==='single'){
    return (document.getElementById('temp_single_input')||{value:''}).value.trim();
  } else if(mode==='range'){
    return (document.getElementById('temp_start_input')||{value:''}).value.trim();
  } else {
    var raw=((document.getElementById('temp_list_input')||{value:''}).value||'');
    return raw.replace(/,/g,' ').trim().split(/\s+/)[0]||'';
  }
}

// Mirror of Python _script_name — runs client-side so user sees the name instantly
function _piShort(piFolder){
  // "wilson-4797-a" → "wilson"  (stop before first all-digit segment)
  var parts=piFolder.split('-'), result=[];
  for(var i=0;i<parts.length;i++){
    if(/^\d+$/.test(parts[i])) break;
    result.push(parts[i]);
  }
  return result.length ? result.join('-') : (parts[0]||'job');
}
function _getScriptName(){
  var root='{{ root_path }}';
  var ud=((document.getElementById('user_dir_input')||{value:''}).value||'').trim().replace(/\/+$/,'');
  if(!ud) return '';
  // Strip trailing numeric component (temperature dir) if present
  var parts=ud.split('/').filter(Boolean);
  if(parts.length && /^\d+$/.test(parts[parts.length-1])) parts.pop();
  ud='/'+parts.join('/');
  var rootSlash=root.endsWith('/')?root:root+'/';
  var rel=ud.startsWith(rootSlash)?ud.slice(rootSlash.length):ud.replace(/^\//,'');
  var piFolder=rel.split('/').filter(Boolean)[0]||'job';
  var piShort=_piShort(piFolder);
  var sample=(parts[parts.length-1])||'sample';
  var temp=_getFirstTemp();
  if(!temp) return '';
  var base=(piShort+'-'+sample+'-'+temp).replace(/[^\w\-]/g,'_');
  return base+'.sh';
}

function updatePathPreview(){
  var el=document.getElementById('path-preview');
  if(!el) return;
  var ud=(document.getElementById('user_dir_input')||{value:''}).value.trim();
  var temp=_getFirstTemp();
  if(!ud){el.style.display='none'; return;}
  var base=ud.endsWith('/')?ud:ud+'/';
  var sname=_getScriptName();
  el.style.display='';
  var html='<div class="pp-label">Path &amp; script name preview</div>';
  if(temp){
    html+='<div><span class="pp-base">'+_escHtml(base)+'</span>'
         +'<span class="pp-temp">'+_escHtml(temp)+'/</span></div>';
  } else {
    html+='<div><span class="pp-base">'+_escHtml(base)+'</span>'
         +'<span style="color:var(--text-muted);font-family:var(--mono);">&lt;TEMP&gt;/</span></div>';
  }
  if(sname){
    html+='<div style="margin-top:5px;">'
         +'<span style="color:var(--text-muted);font-size:11px;">Script name: </span>'
         +'<span style="font-family:var(--mono);color:#a78bfa;font-weight:600;">'+_escHtml(sname)+'</span>'
         +'</div>';
  }
  el.innerHTML=html;
}

function copyScript(){
  var el=document.getElementById('script-pre-content');
  if(!el) return;
  navigator.clipboard.writeText(el.innerText).then(function(){
    var btn=document.getElementById('copy-btn');
    btn.textContent='Copied!';
    setTimeout(function(){btn.textContent='Copy Script';},1500);
  });
}

// ── Directory picker ──────────────────────────────────────────────────────────
var _dpTarget = null;   // which input field to populate

function openDirPicker(targetId){
  _dpTarget = targetId;
  document.getElementById('dir-modal').classList.add('open');
  _dpNav('{{ root_path }}');
}
function closeDirPicker(){
  document.getElementById('dir-modal').classList.remove('open');
}
function _dpNav(path){
  document.getElementById('dp-loading').style.display='';
  document.getElementById('dp-list').innerHTML='';
  fetch('/browse_dir?path='+encodeURIComponent(path))
    .then(function(r){return r.json();})
    .then(function(d){
      document.getElementById('dp-loading').style.display='none';
      // breadcrumbs
      var crumbs=document.getElementById('dp-crumbs');
      crumbs.innerHTML='';
      var parts=d.path.split('/').filter(Boolean);
      var built='';
      parts.forEach(function(p,i){
        built+='/'+p;
        var sp=document.createElement('span');
        sp.className='crumb'; sp.textContent=p;
        (function(cp){sp.onclick=function(){_dpNav(cp);};})(built);
        crumbs.appendChild(sp);
        if(i<parts.length-1){var s=document.createElement('span');s.className='crumb-sep';s.textContent='/';crumbs.appendChild(s);}
      });
      // current path footer
      document.getElementById('dp-cur').textContent=d.path;
      // directory list
      var ul=document.getElementById('dp-list');
      if(d.dirs.length===0){
        ul.innerHTML='<div style="padding:20px;color:var(--text-muted);font-size:12px;text-align:center;">No sub-folders</div>';
      } else {
        d.dirs.forEach(function(dir){
          var row=document.createElement('div');
          row.className='dir-item';
          row.innerHTML='<span style="font-size:16px;">&#128193;</span><span>'+dir.name+'</span>';
          row.onclick=function(){_dpNav(dir.path);};
          ul.appendChild(row);
        });
      }
    })
    .catch(function(e){
      document.getElementById('dp-loading').style.display='none';
      document.getElementById('dp-list').innerHTML='<div style="padding:20px;color:#ef4444;font-size:12px;">Error: '+e+'</div>';
    });
}
function _dpSelect(){
  var p=document.getElementById('dp-cur').textContent;
  var selected=p.endsWith('/')?p:p+'/';
  if(_dpTarget==='user_dir_input'){
    // If last path component is purely numeric (e.g. /91/), it's a temperature dir —
    // strip it from USER_DIR and auto-fill the temperature input instead.
    var trimmed=selected.replace(/\/+$/,'');
    var lastComp=trimmed.split('/').pop();
    if(/^\d+$/.test(lastComp)){
      var parentPath=trimmed.split('/').slice(0,-1).join('/')+'/';
      document.getElementById('user_dir_input').value=parentPath;
      var tempEl=document.getElementById('temp_single_input');
      if(tempEl){ tempEl.value=lastComp; }
      setTempMode('single');
    } else {
      document.getElementById('user_dir_input').value=selected;
    }
    // Auto-fill chmod path as first-level folder below ROOT
    var useDir=document.getElementById('user_dir_input').value;
    var root='{{ root_path }}';
    var rootSlash=root.endsWith('/')?root:root+'/';
    if(useDir.startsWith(rootSlash)){
      var rel=useDir.slice(rootSlash.length);
      var piFolder=rel.split('/')[0];
      if(piFolder){
        var chmodEl=document.getElementById('chmod_path_input');
        if(chmodEl && !chmodEl.value.trim())
          chmodEl.value=rootSlash+piFolder+'/';
      }
    }
    updatePathPreview();
  } else {
    document.getElementById(_dpTarget).value=selected;
  }
  closeDirPicker();
}

// ── Step presets ──────────────────────────────────────────────────────────────
var _PRESETS = {
  'parent_avail': {
    label: 'Parent file (available)',
    steps: {load:1, link_copy:1, link_only:0, link_ow:0, copy_only:0, find:1, refine:1, transform:1, pdf:0}
  },
  'parent_not_avail': {
    label: 'Parent file (not available)',
    steps: {load:1, link_copy:0, link_only:1, link_ow:0, copy_only:0, find:1, refine:0, transform:0, pdf:0}
  },
  'after_parent': {
    label: 'After parent file available',
    steps: {load:0, link_copy:0, link_only:0, link_ow:0, copy_only:1, find:0, refine:1, transform:1, pdf:0}
  }
};
var _activePreset = null;

function applyPreset(key){
  var preset = _PRESETS[key];
  if(!preset) return;
  // Toggle off if already active
  if(_activePreset === key){
    _activePreset = null;
    document.querySelectorAll('.preset-btn').forEach(function(b){b.classList.remove('active');});
    return;
  }
  _activePreset = key;
  // Update preset button highlight
  document.querySelectorAll('.preset-btn').forEach(function(b){
    b.classList.toggle('active', b.dataset.preset===key);
  });
  // Set each step checkbox
  Object.keys(preset.steps).forEach(function(s){
    var el = document.getElementById('step_'+s);
    if(el) el.checked = (preset.steps[s] === 1);
  });
}

document.addEventListener('DOMContentLoaded',function(){
  applyTheme();
  var m=document.getElementById('temp_mode_val');
  if(m) setTempMode(m.value||'single');
  // close modal on overlay click
  document.getElementById('dir-modal').addEventListener('click',function(e){
    if(e.target===this) closeDirPicker();
  });
  // live path preview on typing
  ['user_dir_input','temp_single_input','temp_start_input','temp_list_input'].forEach(function(id){
    var el=document.getElementById(id);
    if(el) el.addEventListener('input', updatePathPreview);
  });
  updatePathPreview();
});
</script>
</head>
<body>

<header>
  <div class="hdr-left">
    <div class="logo" id="hdr-logo">
      <img src="/logo" alt="CHESS"
           onerror="var d=document.getElementById('hdr-logo');d.classList.add('fallback');d.innerHTML='NX';">
    </div>
    <div>
      <div class="hdr-title">NxRefine Job Submission CLASSE Cluster (lnx201) for Quantum Materials Beamline</div>
      <div class="hdr-sub">CHESS · Cornell · SGE Cluster  | Developed by QM2 Beamline Scientist</div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:10px;">
    <span class="badge badge-blue">{{ default_queue }}</span>
    {% if history|length > 0 %}
    <span class="badge badge-gold">{{ history|length }} job{% if history|length!=1 %}s{% endif %} submitted</span>
    {% endif %}
    <button class="theme-btn" onclick="toggleTheme()">&#9790; Theme</button>
  </div>
</header>

<div class="page">

  <!-- ── NxRefine Job Configuration at Quantum Materials (QM2) Beamline ───────────────────────────────────────────────── -->
  <div class="card">
    <div class="card-header">
      <div class="card-title"><div class="dot"></div>NxRefine Job Configuration for Quantum Materials (QM2) Beamline</div>
    </div>

    <form method="POST" action="/">
      <input type="hidden" name="action" id="form-action" value="preview">
      <input type="hidden" name="temp_mode" id="temp_mode_val" value="{{ temp_mode|default('single') }}">

      <!-- Sample directory -->
      <div class="field">
        <label class="field-label">Sample directory (USER_DIR)</label>
        <div style="display:flex;gap:8px;align-items:center;max-width:740px;">
          <input type="text" id="user_dir_input" name="user_dir" value="{{ user_dir }}"
                 placeholder="/nfs/chess/id4baux/2026-1/PI-PROP/nxrefine/SAMPLE/SAMPLE_b1_s1/"
                 style="flex:1;" oninput="updatePathPreview()">
          <button type="button" class="browse-btn" onclick="openDirPicker('user_dir_input')">
            &#128193; Browse
          </button>
        </div>
        <p class="hint">Rooted at <code>{{ root_path }}</code> &mdash; TEMP is appended as a subdirectory name</p>
        <div id="path-preview" class="path-preview"></div>
      </div>

      <!-- Entries -->
      <div class="field">
        <label class="field-label">Frame entries</label>
        <input type="text" name="entries" value="{{ entries|default('f1 f2 f3') }}"
               style="width:200px;" placeholder="f1 f2 f3">
        <p class="hint">Space-separated list of entries to process</p>
      </div>

      <!-- Temperature -->
      <div class="field">
        <label class="field-label">Temperature(s)</label>
        <div class="temp-modes">
          <button type="button" class="mode-btn" id="modeBtn_single" onclick="setTempMode('single')">Single</button>
          <button type="button" class="mode-btn" id="modeBtn_range"  onclick="setTempMode('range')">Range</button>
          <button type="button" class="mode-btn" id="modeBtn_list"   onclick="setTempMode('list')">List</button>
        </div>
        <div id="tblock_single" style="display:none;">
          <input type="number" id="temp_single_input" name="temp_single"
                 value="{{ temp_single|default('') }}"
                 style="width:130px;" placeholder="e.g. 91" oninput="updatePathPreview()">
          <span class="hint" style="display:inline-block;margin-left:8px;">Single temperature value</span>
        </div>
        <div id="tblock_range" style="display:none;">
          <div class="row" style="gap:14px;">
            <div class="field">
              <label class="field-label">Start</label>
              <input type="number" id="temp_start_input" name="temp_start"
                     value="{{ temp_start|default('') }}" style="width:110px;"
                     placeholder="e.g. 19" oninput="updatePathPreview()">
            </div>
            <div class="field">
              <label class="field-label">End</label>
              <input type="number" name="temp_end" value="{{ temp_end|default('') }}" style="width:110px;" placeholder="e.g. 28">
            </div>
            <div class="field">
              <label class="field-label">Step</label>
              <input type="number" name="temp_step" value="{{ temp_step|default('2') }}" style="width:110px;">
            </div>
          </div>
          <p class="hint">Produces: start, start+step, start+2·step, … up to end</p>
        </div>
        <div id="tblock_list" style="display:none;">
          <input type="text" id="temp_list_input" name="temp_list"
                 value="{{ temp_list|default('100,105,110') }}"
                 style="width:360px;" placeholder="100,105,110"
                 oninput="updatePathPreview()">
          <p class="hint">Comma or space separated values</p>
        </div>
      </div>

      <!-- Processing steps -->
      <div class="field">
        <label class="field-label">Processing steps</label>
        <!-- Quick presets -->
        <div class="preset-bar">
          <span style="font-size:11px;color:var(--text-muted);align-self:center;margin-right:2px;">Quick&nbsp;select:</span>
          <button type="button" class="preset-btn" data-preset="parent_avail"
                  onclick="applyPreset('parent_avail')">&#9654; Parent file (available)</button>
          <button type="button" class="preset-btn" data-preset="parent_not_avail"
                  onclick="applyPreset('parent_not_avail')">&#9654; Parent file (not available)</button>
          <button type="button" class="preset-btn" data-preset="after_parent"
                  onclick="applyPreset('after_parent')">&#9654; After parent file available</button>
        </div>
        <div class="steps-grid">

          <label class="step-card">
            <input type="checkbox" id="step_load" name="step_load" value="1"
                   {% if step_load!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Load</div>
              <div class="step-cmd">--load --overwrite</div>
              <div class="step-desc">Stack / load raw .cbf frames into f1 / f2 / f3</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_link_copy" name="step_link_copy" value="1"
                   {% if step_link_copy!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Link + Copy + Max</div>
              <div class="step-cmd">--link --copy --max</div>
              <div class="step-desc">Link, copy orientation from parent file, compute max</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_link_only" name="step_link_only" value="1"
                   {% if step_link_only=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">Link only + Max</div>
              <div class="step-cmd">--link --max</div>
              <div class="step-desc">Use when no parent file is available</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_link_ow" name="step_link_ow" value="1"
                   {% if step_link_ow=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">Link only (overwrite)</div>
              <div class="step-cmd">--link --overwrite</div>
              <div class="step-desc">Re-link with overwrite (no max, no copy)</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_copy_only" name="step_copy_only" value="1"
                   {% if step_copy_only=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">Copy only</div>
              <div class="step-cmd">--copy --overwrite</div>
              <div class="step-desc">Re-run copy if it wasn't completed</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_find" name="step_find" value="1"
                   {% if step_find!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Find peaks</div>
              <div class="step-cmd">nxfind &nbsp;-t &nbsp;
                <input type="number" name="find_threshold"
                       value="{{ find_threshold|default('80000') }}"
                       style="width:90px;padding:2px 6px;font-size:11px;display:inline-block;"
                       min="1000" step="1000"
                       onclick="event.stopPropagation()" onchange="event.stopPropagation()">
              </div>
              <div class="step-desc">Find Bragg peaks above threshold</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_refine" name="step_refine" value="1"
                   {% if step_refine!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Refine</div>
              <div class="step-cmd">--refine --overwrite</div>
              <div class="step-desc">Refine the orientation matrix</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_transform" name="step_transform" value="1"
                   {% if step_transform!='0' %}checked{% endif %}>
            <div>
              <div class="step-name">Transform + Combine</div>
              <div class="step-cmd">--transform --combine --regular</div>
              <div class="step-desc">HKL transform and combine all frames</div>
            </div>
          </label>

          <label class="step-card">
            <input type="checkbox" id="step_pdf" name="step_pdf" value="1"
                   {% if step_pdf=='1' %}checked{% endif %}>
            <div>
              <div class="step-name">3D PDF</div>
              <div class="step-cmd">--pdf --regular</div>
              <div class="step-desc">Generate 3D pair distribution function (slow)</div>
            </div>
          </label>

        </div>
      </div>

      <div class="divider"></div>

      <!-- Queue options -->
      <div class="field">
        <label class="field-label">Queue / resources</label>
        <div class="row">
          <div class="field">
            <label class="field-label">Queue</label>
            <input type="text" name="q_queue" value="{{ q_queue|default('all.q') }}" style="width:160px;">
          </div>
          <div class="field">
            <label class="field-label">mem_free</label>
            <input type="text" name="q_mem" value="{{ q_mem|default('200G') }}" style="width:90px;">
          </div>
          <div class="field">
            <label class="field-label">Parallel env</label>
            <input type="text" name="q_pe" value="{{ q_pe|default('sge_pe') }}" style="width:110px;">
          </div>
          <div class="field">
            <label class="field-label">Cores</label>
            <input type="number" name="q_cores" value="{{ q_cores|default('8') }}" style="width:80px;" min="1">
          </div>
        </div>
      </div>

      <!-- Email notification -->
      <div class="field">
        <label class="field-label">Email notification <span style="font-weight:400;text-transform:none;">(optional)</span></label>
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
          <input type="text" name="notify_email" value="{{ notify_email|default('') }}"
                 placeholder="your@email.com" style="width:280px;">
          <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" name="notify_end"   value="1" {% if notify_end  !='0' %}checked{% endif %}> Job end
          </label>
          <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" name="notify_abort" value="1" {% if notify_abort=='1' %}checked{% endif %}> Abort / error
          </label>
          <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer;">
            <input type="checkbox" name="notify_begin" value="1" {% if notify_begin=='1' %}checked{% endif %}> Job start
          </label>
        </div>
        <p class="hint">Adds <code>#$ -M</code> / <code>#$ -m</code> SGE directives to the script header.</p>
      </div>

      <!-- Script / chmod paths -->
      <div class="field">
        <label class="field-label">Script output directory</label>
        <div style="display:flex;gap:8px;align-items:center;max-width:740px;">
          <input type="text" id="script_dir_input" name="script_dir" value="{{ script_dir }}"
                 placeholder="/nfs/chess/id4baux/2026-1/PI-PROP/script/after/"
                 style="flex:1;">
          <button type="button" class="browse-btn" onclick="openDirPicker('script_dir_input')">
            &#128193; Browse
          </button>
        </div>
        <p class="hint">Where to save the <code>.sh</code> file. Leave blank to save alongside USER_DIR.</p>
      </div>

      <div class="field">
        <label class="field-label">chmod path <span style="font-weight:400;text-transform:none;">(optional &mdash; auto-filled from USER_DIR)</span></label>
        <input type="text" id="chmod_path_input" name="chmod_path" value="{{ chmod_path }}"
               placeholder="/nfs/chess/id4baux/2026-1/PI-PROP/"
               style="max-width:620px;">
        <p class="hint">Runs <code>chmod -R 777</code> on this path at the end of the script. Auto-filled to the PI folder when you browse for USER_DIR.</p>
      </div>

      <!-- Action buttons -->
      <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;">
        <button type="submit" class="btn btn-primary"
                onclick="document.getElementById('form-action').value='preview'">
          &#128196; Generate Script
        </button>
        <button type="submit" class="btn btn-submit"
                onclick="document.getElementById('form-action').value='submit'">
          &#9654; Generate &amp; Submit Job
        </button>
      </div>
    </form>
  </div>

  <!-- ── Validation errors ────────────────────────────────────────────────── -->
  {% if errors %}
  <div class="card" style="border-color:rgba(239,68,68,.35);">
    {% for e in errors %}<div class="alert-error" style="margin-bottom:6px;">&#9888; {{ e }}</div>{% endfor %}
  </div>
  {% endif %}

  <!-- ── Generated script preview ─────────────────────────────────────────── -->
  {% if script_text %}
  <div class="card">
    <div class="card-header">
      <div class="card-title"><div class="dot dot-gold"></div>Generated Script</div>
      <div style="display:flex;align-items:center;gap:10px;">
        <code style="font-size:11px;">{{ script_path }}</code>
        <button id="copy-btn" class="btn btn-clear" style="padding:4px 12px;font-size:11px;" onclick="copyScript()">
          Copy Script
        </button>
      </div>
    </div>
    <div class="script-pre" id="script-pre-content">{{ script_text }}</div>
    <div class="qsub-bar">
      <span class="qsub-label">qsub command:</span>
      <span style="color:#38bdf8;">{{ qsub_cmd }}</span>
    </div>
  </div>
  {% endif %}

  <!-- ── Submission result ─────────────────────────────────────────────────── -->
  {% if job_output is not none %}
  <div class="card" style="border-color:{% if job_ok %}rgba(34,197,94,.35){% else %}rgba(239,68,68,.35){% endif %};">
    <div class="card-header">
      <div class="card-title">
        <div class="dot {% if job_ok %}dot-green{% else %}dot-red{% endif %}"></div>
        Submission {% if job_ok %}Successful{% else %}Failed{% endif %}
      </div>
    </div>
    {% if job_ok %}
    <p class="alert-success">&#10003; {{ job_output }}</p>
    {% else %}
    <p class="alert-error">{{ job_output }}</p>
    {% endif %}
    {% if job_stderr %}
    <pre style="margin-top:10px;font-size:11px;color:var(--text-muted);">{{ job_stderr }}</pre>
    {% endif %}
  </div>
  {% endif %}

  <!-- ── Job history ───────────────────────────────────────────────────────── -->
  {% if history %}
  <div class="card">
    <div class="card-header">
      <div class="card-title"><div class="dot"></div>Job History <span class="badge badge-blue" style="margin-left:6px;">{{ history|length }}</span></div>
      <form method="POST" action="/clear_history" style="margin:0;">
        <button type="submit" class="btn btn-clear" style="padding:4px 12px;font-size:11px;">Clear</button>
      </form>
    </div>
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Script</th>
          <th>Temperatures</th>
          <th>Steps</th>
          <th>Result</th>
        </tr>
      </thead>
      <tbody>
        {% for j in history|reverse %}
        <tr>
          <td style="white-space:nowrap;color:var(--text-muted);">{{ j.time }}</td>
          <td style="font-size:11px;word-break:break-all;max-width:220px;">{{ j.script_name }}</td>
          <td style="white-space:nowrap;">{{ j.temps }}</td>
          <td style="font-size:11px;color:var(--text-muted);">{{ j.steps }}</td>
          <td>
            {% if j.ok %}
            <span class="job-ok">&#10003; {{ j.job_id }}</span>
            {% else %}
            <span class="job-fail">&#10007; failed</span>
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
  {% endif %}

</div><!-- /page -->

<!-- ── Directory picker modal ──────────────────────────────────────────────── -->
<div class="modal-overlay" id="dir-modal">
  <div class="modal-box">
    <div class="modal-header">
      <span class="modal-title">&#128193; Choose Directory</span>
      <button class="modal-close" onclick="closeDirPicker()">&#215;</button>
    </div>
    <div class="modal-crumbs" id="dp-crumbs"></div>
    <div class="modal-body">
      <div id="dp-loading" style="padding:30px;text-align:center;color:var(--text-muted);font-size:13px;">
        Loading&hellip;
      </div>
      <div id="dp-list"></div>
    </div>
    <div class="modal-footer">
      <span class="modal-cur" id="dp-cur"></span>
      <button type="button" class="btn btn-clear" style="padding:6px 14px;font-size:12px;" onclick="closeDirPicker()">Cancel</button>
      <button type="button" class="btn btn-primary" style="padding:6px 16px;font-size:12px;" onclick="_dpSelect()">
        &#10003; Select This Folder
      </button>
    </div>
  </div>
</div>

</body>
</html>
"""

# ── Helpers ───────────────────────────────────────────────────────────────────
import re as _re

def _script_name(user_dir, temps_str):
    """Build a human-readable script filename, e.g. wilson-CeCd3P3_b3_s1-91.sh"""
    ud = user_dir.rstrip("/")
    # Strip trailing numeric temperature directory if the full path was entered
    # e.g. .../CeCd3P3_b3_s1/91/ → use .../CeCd3P3_b3_s1 as the base
    _last = os.path.basename(ud)
    if _last.lstrip('-').isdigit() and _last:
        ud = os.path.dirname(ud).rstrip("/")
    # PI short name: first component below ROOT, strip trailing -digits, e.g. wilson-4797-a → wilson
    try:
        rel = ud[len(ROOT):].lstrip("/") if ud.startswith(ROOT) else ud.lstrip("/")
        pi_folder = rel.split("/")[0] if rel else ""
        pi_short  = _re.split(r'-\d', pi_folder)[0] if pi_folder else "job"
    except Exception:
        pi_short = "job"
    # Sample name = last path component of USER_DIR
    sample = os.path.basename(ud) if ud else "sample"
    # Compact temperature string
    parts = temps_str.strip().split()
    if len(parts) == 1:
        tc = parts[0]
    elif len(parts) <= 6:
        tc = "_".join(parts)
    else:
        tc = f"{parts[0]}-{parts[-1]}"
    base = _re.sub(r'[^\w\-]', '_', f"{pi_short}-{sample}-{tc}")
    # Avoid overwriting: append _2, _3 … if the file already exists
    return base


def _build_script(user_dir, temps_str, entries, steps, threshold, chmod_path,
                  notify_email="", notify_end="0", notify_abort="0", notify_begin="0"):
    ent = entries.strip() or "f1 f2 f3"
    ud  = user_dir.rstrip("/") + "/"
    # SGE email directives
    email_lines = []
    if notify_email.strip():
        email_lines.append(f"#$ -M {notify_email.strip()}")
        flags = ""
        if notify_begin == "1": flags += "b"
        if notify_end   == "1": flags += "e"
        if notify_abort == "1": flags += "a"
        if flags:
            email_lines.append(f"#$ -m {flags}")
    L = [
        "#!/bin/bash",
    ] + email_lines + [
        "LOCAL_DIR=${TMPDIR}",
        "echo running on host: `hostname`",
        'echo `date` "USER ${USER} JOB_ID ${JOB_ID}"',
        "source /nfs/chess/id4baux/nxserver/nxsetup.sh",
        "",
        "function nxrefine_process_temp () {",
        "    echo ${USER_DIR}${TEMP}",
    ]
    base = "    nxreduce --directory ${USER_DIR}${TEMP}"
    if "load"       in steps: L.append(f"{base} --entries {ent} --load --overwrite")
    if "link_copy"  in steps: L.append(f"{base} --entries {ent} --link --copy --max")
    if "link_only"  in steps: L.append(f"{base} --entries {ent} --link --max")
    if "link_ow"    in steps: L.append(f"{base} --entries {ent} --link --overwrite")
    if "copy_only"  in steps: L.append(f"{base} --entries {ent} --copy --overwrite")
    if "find"       in steps: L.append(f"    nxfind --directory ${{USER_DIR}}${{TEMP}} --entries {ent} -t {threshold} --overwrite")
    if "refine"     in steps: L.append(f"{base} --entries {ent} --refine --overwrite")
    if "transform"  in steps: L.append(f"{base} --entries {ent} --transform --combine --regular --overwrite")
    if "pdf"        in steps: L.append(f"    nxreduce --directory ${{USER_DIR}}${{TEMP}} --pdf --regular")
    L += [
        "}",
        "",
        f"USER_DIR='{ud}'",
        f"for TEMP in {temps_str};",
        "do",
        "    nxrefine_process_temp $TEMP",
        "done",
    ]
    if chmod_path and chmod_path.strip():
        L.append(f"chmod -R 777 {chmod_path.strip()}")
    return "\n".join(L) + "\n"


def _parse_temps(mode, single, start, end, step, lst):
    try:
        if mode == "single":
            return str(int(float(single or "0"))), None
        if mode == "range":
            s, e, st = int(float(start or "0")), int(float(end or "0")), int(float(step or "1"))
            if st == 0: return None, "Step cannot be zero."
            vals = list(range(s, e + (1 if st > 0 else -1), st))
            if not vals: return None, "Range produces no values."
            return " ".join(str(v) for v in vals), None
        # list mode
        raw  = (lst or "").replace(",", " ").split()
        vals = [int(float(v)) for v in raw if v.strip()]
        if not vals: return None, "Temperature list is empty."
        return " ".join(str(v) for v in vals), None
    except Exception as ex:
        return None, f"Temperature parse error: {ex}"


# ── Routes ────────────────────────────────────────────────────────────────────
def _render(**ctx):
    with _history_lock:
        ctx["history"] = list(_job_history)
    ctx.setdefault("root_path", ROOT)
    ctx.setdefault("default_queue", "all.q")
    return render_template_string(PAGE, **ctx)


@app.route("/logo")
def serve_logo():
    """Serve the CHESS logo from the NFS path."""
    if os.path.isfile(CHESS_LOGO):
        return send_file(CHESS_LOGO, mimetype="image/png")
    # Return a transparent 1×1 PNG so the img onerror fallback fires
    import base64
    _blank = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
    return Response(_blank, mimetype="image/png")


@app.route("/browse_dir")
def browse_dir():
    """Return JSON list of immediate subdirectories under path, confined to ROOT."""
    path = request.args.get("path", ROOT)
    path = os.path.normpath(path)
    # Safety: never escape ROOT
    if not path.startswith(ROOT):
        path = ROOT
    dirs = []
    try:
        for name in sorted(os.listdir(path)):
            full = os.path.join(path, name)
            if os.path.isdir(full):
                dirs.append({"name": name, "path": full})
    except PermissionError:
        pass
    except FileNotFoundError:
        path = ROOT
    return jsonify({"path": path, "dirs": dirs})


@app.route("/", methods=["GET"])
def index():
    return _render(
        errors=[], script_text=None, script_path=None, qsub_cmd=None,
        job_output=None, job_stderr=None, job_ok=False,
        # default form values
        user_dir="", entries="f1 f2 f3", temp_mode="single",
        temp_single="", temp_start="", temp_end="", temp_step="1",
        temp_list="100,105,110", find_threshold="80000",
        q_queue="all.q", q_mem="200G", q_pe="sge_pe", q_cores="8",
        script_dir="", chmod_path="",
        notify_email="", notify_end="1", notify_abort="0", notify_begin="0",
        step_load="1", step_link_copy="1", step_link_only="0",
        step_link_ow="0", step_copy_only="0", step_find="1", step_refine="1",
        step_transform="1", step_pdf="0",
    )


@app.route("/", methods=["POST"])
def submit():
    fv = {k: request.form.get(k, "") for k in [
        "user_dir","entries","temp_mode","temp_single","temp_start","temp_end",
        "temp_step","temp_list","find_threshold","q_queue","q_mem","q_pe",
        "q_cores","script_dir","chmod_path","notify_email",
    ]}
    for s in ["load","link_copy","link_only","link_ow","copy_only","find","refine","transform","pdf"]:
        fv[f"step_{s}"] = request.form.get(f"step_{s}", "0")
    for n in ["notify_end","notify_abort","notify_begin"]:
        fv[n] = request.form.get(n, "0")
    action = request.form.get("action", "preview")

    errors, script_text, script_path, qsub_cmd = [], None, None, None
    job_output, job_stderr, job_ok = None, None, False

    # ── validate ──────────────────────────────────────────────────────────────
    user_dir  = fv["user_dir"].strip()

    # Auto-normalize: if USER_DIR ends with a numeric directory (e.g. .../CeCd3P3_b3_s1/91/)
    # strip that component and use it as the temperature, so the script name is correct.
    _ud_last = os.path.basename(user_dir.rstrip("/"))
    if _ud_last.lstrip('-').isdigit() and _ud_last:
        user_dir = os.path.dirname(user_dir.rstrip("/")).rstrip("/") + "/"
        fv["user_dir"] = user_dir   # fix the re-rendered form value too
        # Auto-fill single temperature if none was entered
        if not fv.get("temp_single","").strip():
            fv["temp_single"] = _ud_last
            fv["temp_mode"]   = "single"

    if not user_dir.strip("/"):
        errors.append("Sample directory (USER_DIR) is required.")

    steps = [s for s in ["load","link_copy","link_only","link_ow","copy_only","find","refine","transform","pdf"]
             if fv.get(f"step_{s}") == "1"]
    if not steps:
        errors.append("Select at least one processing step.")

    temps_str, terr = _parse_temps(
        fv["temp_mode"], fv["temp_single"], fv["temp_start"],
        fv["temp_end"],  fv["temp_step"],  fv["temp_list"])
    if terr:
        errors.append(terr)

    if not errors:
        threshold  = fv["find_threshold"].strip() or "80000"
        chmod_path = fv["chmod_path"].strip()

        # ── build script ──────────────────────────────────────────────────────
        script_text = _build_script(
            user_dir, temps_str, fv["entries"], steps, threshold, chmod_path,
            notify_email=fv.get("notify_email",""),
            notify_end=fv.get("notify_end","0"),
            notify_abort=fv.get("notify_abort","0"),
            notify_begin=fv.get("notify_begin","0"),
        )

        # ── save script ───────────────────────────────────────────────────────
        sdir  = fv["script_dir"].strip()
        if not sdir:
            parent = os.path.dirname(user_dir.rstrip("/")) if user_dir.rstrip("/") else ""
            sdir   = os.path.join(parent, "scripts") if parent else "/tmp/nxreduce_scripts"
        base_name   = _script_name(user_dir, temps_str)
        # Avoid overwriting existing scripts: add _2, _3 … suffix
        sname       = base_name + ".sh"
        candidate   = os.path.join(sdir, sname)
        _counter    = 2
        try:
            os.makedirs(sdir, exist_ok=True)
            while os.path.exists(candidate):
                sname     = f"{base_name}_{_counter}.sh"
                candidate = os.path.join(sdir, sname)
                _counter += 1
        except Exception:
            pass  # makedirs error handled below
        script_path = candidate
        _save_warn  = None
        try:
            os.makedirs(sdir, exist_ok=True)
            with open(script_path, "w") as f:
                f.write(script_text)
            os.chmod(script_path, 0o755)
        except Exception as _io_err:
            # Fall back to /tmp so the user can still see and copy the script
            _fallback = f"/tmp/nxreduce_scripts"
            try:
                os.makedirs(_fallback, exist_ok=True)
                script_path = os.path.join(_fallback, sname)
                with open(script_path, "w") as f:
                    f.write(script_text)
                os.chmod(script_path, 0o755)
                _save_warn = (f"Could not write to {sdir!r} ({_io_err}). "
                              f"Script saved to fallback: {script_path}")
            except Exception as _fb_err:
                errors.append(f"Script save failed: {_io_err} | fallback also failed: {_fb_err}")
                script_text = None
        if _save_warn:
            errors.append(_save_warn)

        # ── build qsub command ────────────────────────────────────────────────
        if script_text is not None:
            q, mem, pe, cores = (fv["q_queue"].strip()  or "all.q",
                                 fv["q_mem"].strip()    or "200G",
                                 fv["q_pe"].strip()     or "sge_pe",
                                 fv["q_cores"].strip()  or "8")
            qsub_cmd = f"qsub -q {q} -l mem_free={mem} -pe {pe} {cores} {script_path}"

        # ── submit ────────────────────────────────────────────────────────────
        if action == "submit" and script_text is not None:
            try:
                res = subprocess.run(qsub_cmd.split(),
                                     capture_output=True, text=True, timeout=30)
                job_output = res.stdout.strip() or "(no stdout)"
                job_stderr = res.stderr.strip()
                job_ok     = res.returncode == 0
            except FileNotFoundError:
                job_output = ("qsub not found — is the SGE environment loaded? "
                              "Run: source /nfs/chess/id4baux/nxserver/nxsetup.sh")
                job_ok = False
            except Exception as ex:
                job_output = str(ex)
                job_ok     = False

            # ── record in history ─────────────────────────────────────────────
            step_labels = {
                "load":"Load","link_copy":"Link+Copy","link_only":"Link+Max",
                "link_ow":"Link(ow)","copy_only":"Copy","find":"Find",
                "refine":"Refine","transform":"Transform","pdf":"PDF"}
            with _history_lock:
                _job_history.append({
                    "time":        datetime.datetime.now().strftime("%H:%M:%S"),
                    "script_name": sname,
                    "temps":       temps_str[:40] + ("…" if len(temps_str) > 40 else ""),
                    "steps":       ", ".join(step_labels[s] for s in steps),
                    "ok":          job_ok,
                    "job_id":      job_output.split("\n")[0][:60] if job_ok else "",
                })

    return _render(errors=errors, script_text=script_text,
                   script_path=script_path, qsub_cmd=qsub_cmd,
                   job_output=job_output, job_stderr=job_stderr, job_ok=job_ok,
                   **fv)


@app.route("/clear_history", methods=["POST"])
def clear_history():
    with _history_lock:
        _job_history.clear()
    from flask import redirect
    return redirect("/")


if __name__ == "__main__":
    import datetime
    app.run(host="0.0.0.0", port=5050, debug=False)