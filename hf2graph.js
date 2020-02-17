#!/usr/bin/env node

// read HyperFlow workflow graph from JSON to a graphlib structure
// create two graphs: full (processes and signals) and one with only processes

var Graph = require("@dagrejs/graphlib").Graph;
 
var createGraph = function(wfjson) {
  let wf = wfjson;
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

  var wfgraph = new Graph();

  processes.forEach(function(proc, idx) {
    wfgraph.setNode("p:"+(idx+1), proc.name);
  });
  signals.forEach(function(sig, idx) {
    wfgraph.setNode("s:"+(idx+1), sig.name);
  });
  processes.forEach(function(proc, idx) {
    (proc.ins||[]).forEach(function(insig) {
      wfgraph.setEdge("s:"+insig, "p:"+(idx+1));
    });
    (proc.outs||[]).forEach(function(outsig) {
      wfgraph.setEdge("p:"+(idx+1), "s:"+outsig)
    });
  });
  
  // then build a smaller graph with only processes (better for visualization)
  var procg = new Graph();

  processes.forEach(function(proc, idx) {
    procg.setNode("p:"+(idx+1), proc.name);
  });

  processes.forEach(function(proc, idx) {
    var children = wfgraph.successors("p:"+(idx+1)).flatMap(x => wfgraph.successors(x));
    children.map(ch => procg.setEdge("p:"+(idx+1), ch));
  });

  return {
    fullGraph: wfgraph,
    procGraph: procg
  }
}

module.exports = createGraph;
