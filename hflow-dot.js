#!/usr/bin/env node

var fs       = require('fs'),
    graphviz = require('graphviz'),
    Graph = require("@dagrejs/graphlib").Graph,
    colormap = require('colormap'),
    docopt = require('docopt').docopt,
    hfgraph = require('./hf2graph.js');

var doc = "\
hflow-dot: converts HyperFlow workflow.json to graphviz dot format\n\
Usage:\n\
  hflow-dot [-p] [--png] <workflow-json-file-path>\n\
  hflow-dot -h|--help\n\
  \
Options:\n\
  -h --help   Prints this\n\
  --png       Generate png\n\
  -p          Generates graph according to paritioning info";

var opts = docopt(doc);

var partitioning = opts['-p'];
var png = opts['--png'];

var file = opts['<workflow-json-file-path>'];

var fileContents = fs.readFileSync(file);

var wf = JSON.parse(fileContents);

var g = graphviz.digraph("G");

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
}

// get a graphlib representation of the workflow process graph
var procg = hfgraph(wf).procGraph;

// generate colormap
var procNames = {}; 
processes.forEach(function(proc, idx) {
  procNames[proc.name]="1";
});
var procNamesArray = Object.keys(procNames).sort((a,b) => a.substring(1).localeCompare(b.substring(1)));
procNamesArray.forEach((pn, idx) => procNames[pn]=idx); // map name to index

let nColors = partitioning ? 2: procNamesArray.length;
let cmap = nColors > 10 ? 'hsv': 'jet';
if (nColors < 6) { cmap = 'autumn'; }
let colors = colormap({
    colormap: cmap,
    nshades: nColors,
    format: 'hex',
    alpha: 1
});

// build the graphviz graph and save it
procg.nodes().forEach(function(proc, idx) {
  var n = g.addNode(proc);
  var name = procg.node(proc);
  let procId = Number(proc.split(':')[1])-1;
  if (partitioning) {
    n.set('label', "");
    let parNum = processes[procId].partition-1;
    n.set('color', colors[parNum]);
  } else {
    n.set('label', name);
    n.set('color', colors[procNames[name]]);
  }
  // special storage node
  if (processes[procId].type == "special") {
    n.set('shape', 'cylinder');
    n.set('width', 1.0);
    n.set('height', 1.0);
  }
  n.set('style', 'filled');
  procg.successors(proc).forEach(function(succ) {
    g.addEdge(proc, succ);
  });
});

let basename = file.substring(0, file.lastIndexOf('.'));

if (png) {
  g.output( {
    "type": "png",
    "use": "dot",
  }, basename + ".png")
} else {
  fs.writeFileSync(basename + ".dot", g.to_dot());
}
