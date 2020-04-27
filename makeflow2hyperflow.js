/* HyperFlow engine. 
 ** Converts from Makeflow mf/json file to Hyperflow workflow representation (json)
 ** Author: Bartosz Balis (2020)
 */

/*
 * Any converter should provide an object constructor with the following API:
 * convert(wf, cb) 
 *   - @wf  - native workflow representation
 *   - @cb  - callback function (err, wfJson)
 * convertFromFile(filename, cb)
 *   - @filename  - file path from which the native wf representation should be read
 *   - @cb        - callback function (err, wfJson)
 */
var fs = require('fs');
const readline = require('readline');

var MakeflowConverter = function(functionName) {
    if (typeof(functionName) === 'undefined') {
        this.functionName = "command_print";
    } else {
        this.functionName = functionName;
    }
}

function getMfJson(filename, cb) {
    var ext = filename.substr(filename.lastIndexOf('.') + 1);
    if (ext == "mf") {
        mf2json(filename, function(mfJson){
            return cb(mfJson);
        });
    } else if (ext == "json") {
        var mfJson;
        try {
            const jsonString = fs.readFileSync(filename);
            let mfJson = JSON.parse(jsonString);
            return cb(mfJson);
        } catch(err) {
            throw(err);
        }
    } else {
        throw("Makeflow2Hyperflow: unrecognized makeflow file extension.")
    }
}

MakeflowConverter.prototype.convertFromFile = function(filename, cb) {
    var that = this;
    getMfJson(filename, function(mfJson) {
        createWorkflow(mfJson, that.functionName, function(err, wfJson) {
            cb(null, wfJson);
        });
    });
}

var sources = {}, sinks = {};
var nextDataId = -1, dataNames = {};

function createWorkflow(wf_mfjson, functionName, cb) {
    var wfOut = {
        //functions: [ {"name": functionName, "module": "functions"} ],
        processes: [],
        signals: [],
        ins: [],
        outs: []
    }

    wf_mfjson.rules.forEach(function(mfproc, procId) {
        var cmd = mfproc.command.split(" ");
        var executable = cmd[0];
        if (executable.substring(0,2) == "./") { executable = executable.substring(2); }
        var args = cmd.slice(1);

        wfOut.processes.push({
            "name": executable,
            "function": "{{function}}",
            "type": "dataflow",
            "firingLimit": 1,
            "config": {
                "executor": {
                    "executable": executable,
                    "args": args
                }
            },
            "ins": [],
            "outs": []
        });

        // check for stdout/stderr redirections
        var stdoutIdx = args.indexOf(">");
        var stderrIdx = args.indexOf("2>");
        if (stdoutIdx != -1) {
            wfOut.processes[procId].config.executor.stdout = args[stdoutIdx+1]
            delete args[stdoutIdx];
            delete args[stdoutIdx+1];
        }
        if (stderrIdx != -1) {
            wfOut.processes[procId].config.executor.stderr = args[stderrIdx+1]
            delete args[stderrIdx];
            delete args[stderrIdx+1];
        }
        wfOut.processes[procId].config.executor.args = args.filter(val => val != null);

        var dataId, dataName;
        (mfproc.inputs || []).forEach(function(dataName) {
            if (dataName != executable) {
                if (!dataNames[dataName]) {
                    ++nextDataId;
                    wfOut.signals.push({
                        "name": dataName,
                        "sources": [],
                        "sinks": []
                    });
                    dataId = nextDataId;
                    dataNames[dataName] = dataId;
                } else {
                    dataId = dataNames[dataName];
                }
                wfOut.processes[procId].ins.push(dataId);
                wfOut.signals[dataId].sinks.push(procId);
            }
        });

        (mfproc.outputs || mfproc.output || []).forEach(function(dataName) {
            if (!dataNames[dataName]) {
                ++nextDataId;
                wfOut.signals.push({
                    "name": dataName,
                    "sources": [],
                    "sinks": []
                });
                dataId = nextDataId;
                dataNames[dataName] = dataId;
            } else {
                dataId = dataNames[dataName];
            }
            wfOut.processes[procId].outs.push(dataId);
            wfOut.signals[dataId].sources.push(procId);
        });
    });

    for (var i=0; i<wfOut.signals.length; ++i) {
        if (wfOut.signals[i].sources.length == 0) {
            wfOut.ins.push(i);
            // the line below sends initial signals to the workflow. FIXME: add a cmd line option 
            wfOut.signals[i].data = [{ }]; 
        }
        if (wfOut.signals[i].sinks.length == 0) {
            wfOut.outs.push(i);
        }
    }

    for (var i=0; i<wfOut.signals.length; ++i) {
        if (wfOut.signals[i].sources.length > 1) {
            console.error("WARNING multiple sources for:" + wfOut.signals[i].name, "sinks:", wfOut.signals[i].sinks);
        }
        delete wfOut.signals[i].sources;
        delete wfOut.signals[i].sinks;
    }

    cb(null, wfOut);
}

function mf2json(mf, cb) {
    var procs = [];
    var dependencies = [];
    var commands = [];
    var jsonOut = { "rules": [] }

    const depRegex = /.*:.*/
    const commandRegex = /\t.*/

    const readInterface = readline.createInterface({
        input: fs.createReadStream(process.argv[2]),
        //output: process.stdout,
        console: false
    });

    readInterface.on('line', function(line) {
        if (line.match(depRegex)) {
            dependencies.push(line);
        }
        if (line.match(commandRegex)) {
            commands.push(line);
        }
    });

    readInterface.on('close', function(line) {
        dependencies.forEach(function(dr, i) {
            jsonOut.rules.push({});
            let deps = dependencies[i].trim().split(':');
            let ins = deps[1].trim().split(' ');
            let outs = deps[0].trim().split(' ');
            let cmd = commands[i].trim().split(' ');
            if (cmd[0] == "LOCAL") { 
                cmd.shift(); 
                jsonOut.rules[i]["local_job"] = true;
            }
            jsonOut.rules[i].command = cmd.join(' ');
            jsonOut.rules[i].inputs = ins;
            jsonOut.rules[i].outputs = outs;
        });
        cb(jsonOut);
    });
}

module.exports = MakeflowConverter;
