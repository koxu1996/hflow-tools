#!/usr/bin/env node

/** reads HyperFlow workflow graph and returns useful information:
 * - nLevels: how many levels the workflow graph has
 * - levelCounts: number of workflow processes per each level
 * - partitioningPerPhase: number of worklow processes per phase per partition
 * 
 * - wfjson: workflow json annotated with the following additional information:
 *   - processes[i].phase = graph level (execution phase) at which process "i" is located
 *   - processes[i].partition = partition to which process "i" belongs
 * 
**/

var fs = require('fs'),
    hfgraph = require('./hf2graph.js');

var partitioningPerPhase;

var computePartitions = function(parfile) {
    var partitions = fs.readFileSync(parfile, 'utf8').trim().split("\n");

   var nPartitions = 0;
    wf.processes.forEach((proc, idx) => {
        let p = Number(partitions[idx]);
        if (nPartitions < p) { nPartitions = p; }
        wf.processes[idx].partition = p+1;
    });
    nPartitions += 1;

    partitioningPerPhase = Array(maxPhase).fill(1).map(p => Array(nPartitions).fill(0)); 
    processes.forEach(p => { 
        partitioningPerPhase[p.phase-1][p.partition-1]++;
    });
}

var main = function(wf, parfile) {
    var signals   = (wf.data      || wf.signals);
    var processes = (wf.processes || wf.tasks);

    var sigMap    = {};

    signals.forEach(function(sig, ix) {
        sigMap[sig.name] = ix;
    });

    // add information about workflow *phase* each process belongs to
    // initialize all to phase 1
    var phases = processes.map(p => 1);
    var g = hfgraph(wf).procGraph;

    g.nodes().forEach(proc => {
        let procIdx = Number(proc.split(':')[1])-1;
        let predPhases = g.predecessors(proc).map(p => {
            let pIdx = Number(p.split(':')[1])-1;
            return phases[pIdx];
        });
        phases[procIdx] = Math.max.apply(null, [0].concat(predPhases))+1;
    });

    var maxPhase = Math.max.apply(null, phases);
    var levelCounts = Array(maxPhase).fill(0);

     wf.processes.forEach((proc, idx) => {
        wf.processes[idx].phase = phases[idx];
        levelCounts[phases[idx]-1]++;
    });

    if (parfile) computePartitions(parfile);

    return {
        partitioningPerPhase: partitioningPerPhase,
        nLevels: maxPhase,
        levelCounts: levelCounts,
        wfjson: wf // annotated worklfow json
    }
}

module.exports = main;