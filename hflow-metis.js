#!/usr/bin/env node

var fs       = require('fs'),
    colormap = require('colormap'),
    docopt = require('docopt').docopt,
    hfgraph = require('./hf2graph.js');

var doc = "\
hflow-metis: converts HyperFlow workflow.json to Metis graph format\n\
Usage:\n\
  hflow-metis [--ew] [--nw] <workflow-json-file-path>\n\
  hflow-metis -h|--help\n\
  \
Options:\n\
  -h --help   Prints this\n\
  --ew        Add edge weights (file sizes that need to be transferred)\n\
  --nw        Add node weights (requested cpu)";

var opts = docopt(doc);

var ew = opts['--ew'];
var nw = opts['--nw'];

var file = opts['<workflow-json-file-path>'];

var fileContents = fs.readFileSync(file);

var wf = JSON.parse(fileContents);

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

// get a graphlib representation of the workflow process graph
var procg = hfgraph(file).procGraph;

// generate graph file
console.log(procg.nodeCount(), procg.edgeCount());
procg.nodes().forEach(proc => {
  //console.log(procg.successors(proc).forEach(x => console.log(x.split(':')[1])));
  //process.stdout.write(proc + ' ');
  if (procg.predecessors(proc).length) {
    process.stdout.write(procg.predecessors(proc).map(x => x.split(':')[1]).join(' ') + ' ');
  }
  if (procg.successors(proc).length) {
    process.stdout.write(procg.successors(proc).map(x => x.split(':')[1]).join(' '));
  }
  console.log();
});
