#!/usr/bin/env node

var fs       = require('fs'),
    colormap = require('colormap'),
    docopt = require('docopt').docopt,
    hfgraph = require('./hf2graph.js');

var doc = "\
hflow-metis: converts HyperFlow workflow.json to Metis graph format\n\
Usage:\n\
  hflow-metis [--ew] [--nw] [--ns] [--lw=<npart>] [--pwgts=<pwgts>] <workflow-json-file-path>\n\
  hflow-metis -h|--help\n\
  \
Options:\n\
  -h --help   Prints this\n\
  --ew        Add edge weights (not implemented, probably not needed)\n\
  --nw        Add node weights (requested cpu)\n\
  --ns        Add node size (communication volume)\n\
  --lw=<npart>     Add level weights for 'npart' partitions\n\
  --pwgts=<pwgts>  Partition size weights, e.g. '0.3,0.7'";

var opts = docopt(doc);

var ew = opts['--ew']; 
var nw = opts['--nw']; // node weight -- computation
var ns = opts['--ns']; // node size -- communication cost
var pwgts = opts['--pwgts'];

// partition weights specify that partitions should be of unequal size
// '0.3,0.7' means that relative size of partition 1/2 is 0.3/0.7 respectively
if (pwgts) pwgts = pwgts.split(',');

// 'npart': the number of partitions for which to add special graph level weights 
// these weights look as follows: 0 0 0 1 0 0, where:
// - the number of weights is the number of levels in the graph
// - all weights are 0 except one for the level to which the node belongs
// The purpose of these weights is to ensure balanced partitioning at each
// level of the graph, so that there is task parallelism at each level
// Source article: [...]
var npart = opts['--lw']; 

// parameters to compute the cost of communication (node size)
const bandwidth = 100000000; // bytes/s (for testing)
const latency = 4;           // seconds (for testing)

var file = opts['<workflow-json-file-path>'];

var fileContents = fs.readFileSync(file);

var wf = JSON.parse(fileContents);

var wfinfo = require('./wfinfo.js')(wf);

var signals   = (wf.data      || wf.signals);
var processes = (wf.processes || wf.tasks);

var sigMap    = {};
signals.forEach(function(sig, ix) {
  sigMap[sig.name] = ix;
});

var sigToId = function(sig) {
  if(!isNaN(parseInt(sig))) { 
    return sig;
  } else {
    return sigMap[sig];
  }
};

var getProcSize = function(proc) {
  var pIdx = Number(proc.split(':')[1])-1;
  var size = 0;
  processes[pIdx].outs.forEach(o => {
    if (signals[o].size) size += Number(signals[o].size);
  });
  processes[pIdx].ins.forEach(i => {
    if (signals[i].size) size += Number(signals[i].size);
  });
  if (size == 0) return 1; // if there is no file size info, return 1
  //console.error(size);
  return Math.round(size/bandwidth)+latency; 
}

var printLevelWeights = function(proc) {
  let pIdx = Number(proc.split(':')[1])-1;
  let nLevels = wfinfo.nLevels;
  let levelCounts = wfinfo.levelCounts;
  let wfjson = wfinfo.wfjson;
  let weights = Array(nLevels).fill(0);
  let phase = wfjson.processes[pIdx].phase;

  // Only add weight if at this phase there will be at least one node in each partition
  // To this end, select the smallest part weight and multiply it by the number
  // of processes at this level (phase)
  let minPart = pwgts ? Math.min.apply(null, pwgts.concat(pwgtsToAdd)): 1/npart;
  if (levelCounts[phase-1] * minPart >= 1) {
    weights[phase-1] = 1; 
  }
  process.stdout.write(weights.join(' ') + ' ');
}

// count weights of all partitions (as Metis will) -- required in "printLevelWeights"
// e.g. if pwgts = "0.3" and npart = 3, this will compute pwgts = "0.3,0.35,0.35"
var pwgtsToAdd = [];
if (pwgts) {
  res = Math.round((1.0 - pwgts.reduce((a, b) => Number(a) + Number(b)))*10)/10;
  console.error(res);
  if (res > 0) {
    nPartsToAdd = npart ? npart-pwgts.length: 1;
    pwgtsToAdd = Array(nPartsToAdd).fill(0).map(p => res/nPartsToAdd);
  }
  //pwgts.concat(pwgtsToAdd).forEach((pw, idx) => console.error(idx + ' = ' + pw));
}

if (pwgts) {
  fs.writeFile("pwgts.cfg", pwgts.reduce((s, pw, i) => { 
      return s.concat(i + " = " + pw + "\n")
  }, ""), function(err) {
      if (err) throw err;
  });
}

// get a graphlib representation of the workflow process graph
var procg = hfgraph(wf).procGraph;

// information about weights (for Metis)
var fmt = (ns ? "1": "0") + (nw || npart ? "1": "0") + (ew ? "1": "0");
if (npart) fmt += " " + Number(wfinfo.nLevels + (nw ? 1: 0)); // number of node weigths

// generate graph file
console.log(procg.nodeCount(), procg.edgeCount(), fmt);
procg.nodes().forEach(proc => {
  //console.log(procg.successors(proc).forEach(x => console.log(x.split(':')[1])));
  //process.stdout.write(proc + ' ');
  if (ns) {
    let size = getProcSize(proc);
    process.stdout.write(size + ' ');
  }

  var nodewgt;
  if (nw) {
    try {
      nodewgt = processes[proc.split(':')[1]-1].config.executor.cpuRequest*100;
      if (!nodewgt) nodewgt = 1;
    } catch(err) {
      console.error("Warning, cpuRequest could not be read for process", proc);
      console.error(err);
      nodewgt = 1;
    }
    process.stdout.write(nodewgt + ' ');
  }

  if (npart) {
    printLevelWeights(proc);
  }

  if (procg.predecessors(proc).length) {
    process.stdout.write(procg.predecessors(proc).map(x => x.split(':')[1]).join(' ') + ' ');
  }
  if (procg.successors(proc).length) {
    process.stdout.write(procg.successors(proc).map(x => x.split(':')[1]).join(' '));
  }
  console.log();
});
