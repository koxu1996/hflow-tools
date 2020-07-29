from main import strToDatetime, loadJsonlFile, getWorkflowName, \
  buildJobMap, buildMetricList, getFirstEventDatetime, \
  getLastEventDatetime, getWorkflowSize, getWorkflowVersion
from collections import defaultdict
from natsort import natsorted, ns
import os
import argparse
import matplotlib.pyplot as plt
import numpy as np


PHASE_DEAD = 0
PHASE_RENTING = 1
PHASE_RUNNING = 2
PHASE_DESTROYING = 3


def buildScalerList(scalerLogLines):
    scalerList = []
    for scalerLogLine in scalerLogLines:
        newScalerLogLine = scalerLogLine.copy()
        newScalerLogLine['time'] = strToDatetime(newScalerLogLine['time'])
        scalerList.append(newScalerLogLine)
    return scalerList


def getFirstScalerDatetime(scalerList):
    minTime = None
    for scalerRecord in scalerList:
        time = scalerRecord['time']
        if minTime is None or time < minTime:
            minTime = time
    return minTime


def extractNodesStages(scalerList):

    nodeStages = []
    firstEventTime = getFirstScalerDatetime(scalerList)
    workingNodes = 0
    desiredNodes = 0

    actionMap = defaultdict(list)
    for scalerAction in scalerList:
        actionType = scalerAction['event']
        time = (scalerAction['time'] - firstEventTime).total_seconds()
        actionMap[time].append(actionType)

    for timeOffset, actions in sorted(actionMap.items()):
        for action in actions:
            if action == 'creatingNode':
                desiredNodes += 1
            elif action == 'nodeReady':
                workingNodes += 1
            elif action == 'destroyingNode':
                desiredNodes -= 1
            elif action == 'nodeDeleted':
                workingNodes -= 1
            else:
                raise Exception('Unknown scaler action:', action)

        nodeStages.append({
            'timeOffset': timeOffset,
            'workingNodes': workingNodes,
            'desiredNodes': desiredNodes,
        })

    return nodeStages


def extractNodesPhases(scalerList):

    nodesPhases = defaultdict(list)

    firstEventTime = getFirstScalerDatetime(scalerList)
    knownNodes = set()

    phaseMap = defaultdict(list)
    for scalerAction in scalerList:
        actionType = scalerAction['event']
        nodeName = scalerAction['value']
        knownNodes.add(nodeName)
        time = (scalerAction['time'] - firstEventTime).total_seconds()
        phaseMap[time].append((actionType, nodeName,))

    for nodeName in knownNodes:
        nodesPhases[nodeName] = [{
            'timeOffset': 0.0,
            'phase': PHASE_DEAD,
        }]

    for timeOffset, actions in sorted(phaseMap.items()):
        for action, nodeName in actions:
            if action == 'creatingNode':
                phase = PHASE_RENTING
            elif action == 'nodeReady':
                phase = PHASE_RUNNING
            elif action == 'destroyingNode':
                phase = PHASE_DESTROYING
            elif action == 'nodeDeleted':
                phase = PHASE_DEAD
            else:
                raise Exception('Unknown scaler action:', action)

            nodesPhases[nodeName].append({
                'timeOffset': timeOffset,
                'phase': phase
            })

    return nodesPhases


def visualizeDirScaler(sourceDir, displayOnly):

    scalerPath = os.path.join(sourceDir, 'scaler.jsonl')
    scaler = None
    try:
        scaler = loadJsonlFile(scalerPath)
    except Exception as e:
        print('Note: Scaler logs not found - no graph to render.')
        return

    metricsPath = os.path.join(sourceDir, 'metrics.jsonl')
    metrics = loadJsonlFile(metricsPath)

    jobDescriptionsPath = os.path.join(sourceDir, 'job_descriptions.jsonl')
    jobDescriptions = loadJsonlFile(jobDescriptionsPath)

    workflowName = getWorkflowName(jobDescriptions)
    jobMap = buildJobMap(jobDescriptions)
    metricList = buildMetricList(metrics)
    scalerList = buildScalerList(scaler)
    firstEventTime = getFirstEventDatetime(metricList)
    lastEventTime = getLastEventDatetime(metricList)
    maxTime = (lastEventTime - firstEventTime).total_seconds()

    plt.rc('figure', figsize=(25,15))
    fig, (gnt, gnt2) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [1, 1]})

    #############
    # SUBPLOT 1 #
    #############

    nodeStages = extractNodesStages(scalerList)

    gnt.title.set_text('Resources supply / demand')
    gnt.set_xlabel('Time [s]')
    gnt.set_ylabel('Number of resources')
    gnt.grid(True)
    gnt.set_xlim(0, maxTime)
    #gnt.set_yticks(y_ticks)
    #gnt.set_yticklabels(y_labels)

    working_x = []
    working_y = []
    for stage in nodeStages:
        working_x.append(stage['timeOffset'])
        working_y.append(stage['workingNodes'])
    gnt.plot(working_x, working_y, label='working nodes', drawstyle="steps-post", marker="o", ls="--", color="darkblue", lw=3.0) # GIT blue line

    desired_x = []
    desired_y = []
    for stage in nodeStages:
        desired_x.append(stage['timeOffset'])
        desired_y.append(stage['desiredNodes'])
    gnt.plot(working_x, desired_y, label='desired nodes', drawstyle="steps-post", marker="o",
             color="green", lw=3.0)

    # over-utilized area
    gnt.fill_between(working_x, working_y, desired_y, step="post", alpha=0.7,
                    where=(np.array(working_y) >= np.array(desired_y)), hatch='//',
                    edgecolor="green", facecolor="none")

    # under-utilized area
    gnt.fill_between(working_x, desired_y, working_y, step="post", alpha=0.7,
                    where=(np.array(desired_y) >= np.array(working_y)), hatch="\\\\",
                    edgecolor="darkblue", facecolor="none")

    gnt.legend(loc='best')

    #############
    # SUBPLOT 2 #
    #############

    nodesPhases = extractNodesPhases(scalerList)

    gnt2.title.set_text('Nodes states')
    gnt2.set_xlabel('Time [s]')
    gnt2.set_ylabel('Node')
    gnt2.set_xlim(0, maxTime)

    gnt2.set_yticks(np.arange(0, len(nodesPhases), 1))
    gnt2.set_yticks(np.arange(-.5, len(nodesPhases), 1), minor=True)
    gnt2.set_yticklabels([(key) for key in natsorted(nodesPhases.keys())])

    gnt2.grid(which='minor')
    gnt2.grid(which='major', axis='x')

    phaseTypes = {PHASE_DEAD: 'Inactive', PHASE_RENTING: 'Creating', PHASE_RUNNING: 'Running', PHASE_DESTROYING: 'Destroying'}

    colorsForPhaseType = {
        PHASE_DEAD: '#cccccc',
        PHASE_RENTING: '#ffff66',
        PHASE_RUNNING: '#66ff66',
        PHASE_DESTROYING: '#ff6666',
    }

    usedLabels = set()
    for index, nodeName in enumerate(natsorted(nodesPhases)):

        nodePhases = nodesPhases[nodeName]

        lastTime = None
        lastPhase = None

        for nodePhase in nodePhases:

            time = nodePhase['timeOffset']

            if lastTime != None:
                cColor = colorsForPhaseType[lastPhase]
                cLabel = phaseTypes[lastPhase]
                # prevent duplicated phases at legend
                if cLabel in usedLabels:
                    cLabel = ""
                else:
                    usedLabels.add(cLabel)

                width = time - lastTime
                gnt2.broken_barh([(lastTime, width)], (index*1-0.5, 1,), color=cColor, label=cLabel)
                                # (x, width)   (y_off, height)

            lastTime = time
            lastPhase = nodePhase['phase']

        # plot remaining phase at the end
        width = maxTime - lastTime
        cColor = colorsForPhaseType[lastPhase]
        gnt2.broken_barh([(lastTime, width)], (index*1-0.5, 1,), color=cColor, label=None)


    gnt2.legend(loc='best')

    ##########
    # COMMON #
    ##########

    plt.show()

    ### COMMON

    if displayOnly is True:
        plt.show()
    else:
        wfSize = getWorkflowSize(jobDescriptions)
        wfVersion = getWorkflowVersion(jobDescriptions)
        filename = '{}-{}-{}_scaling.png'.format(workflowName, wfSize, wfVersion)
        plt.savefig(filename)
        print('Scaling chart saved to {}'.format(filename))

    return


def main():
    parser = argparse.ArgumentParser(description='HyperFlow scaler visualizer')
    parser.add_argument('-s', '--source', type=str, required=True, help='Directory with parsed logs')
    parser.add_argument('-d', '--show', action='store_true', default=False, help='Display plot instead of saving to file')
    args = parser.parse_args()
    visualizeDirScaler(args.source, args.show)

if __name__ == '__main__':
    main()
