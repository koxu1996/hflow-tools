#!/usr/bin/env python

from collections import defaultdict
from natsort import natsorted, ns
from scipy import interpolate
import os
import argparse
import jsonlines
import pandas as pd
import datetime
import matplotlib.cbook as cbook
import matplotlib.colors as mc
import matplotlib.collections as mcoll
import colorsys
import math
import matplotlib.pyplot as plt
import numpy as np
import sys


def strToDatetime(dateTimeString):
    pattern = '%Y-%m-%dT%H:%M:%S.%f'
    dateTime = datetime.datetime.strptime(dateTimeString, pattern)
    return dateTime

def buildJobMap(jobDescriptions):
    jobMap = {}
    for row in jobDescriptions:
        jobId = row['jobId']
        if jobId in jobMap:
            raise Exception('Duplicated description for job {}'.format(jobId))
        jobMap[jobId] = row
    return jobMap

def buildMetricList(metrics):
    metricList = []
    for metric in metrics:
        newMetric = metric.copy()
        newMetric['time'] = strToDatetime(newMetric['time'])
        metricList.append(newMetric)
    return metricList

def getLastEventDatetime(metricList):
    maxTime = None
    for metric in metricList:
        if metric['parameter'] != 'event':
            continue
        time = metric['time']
        if maxTime is None or time > maxTime:
            maxTime = time
    return maxTime

def splitJobsIntoDisjointGroups(jobs):

    # firstly we prepare ordered map of jobs (order by start time)
    jobsByStartTime = {}
    for jobId, jobDetails in jobs.items():
        timeStart = jobDetails['handlerStart']
        if timeStart not in jobsByStartTime:
            jobsByStartTime[timeStart] = []
        details = jobDetails.copy()
        details['jobId'] = jobId
        jobsByStartTime[timeStart].append(details)

    unplacedJobsSorted = sorted(jobsByStartTime.items())

    groups = []

    jobs.copy()
    currentEnd = None
    currentGroup = []
    while len(unplacedJobsSorted) != 0:

        # loop for finding job with smallest handlerStart,
        # but greater that last handlerEnd in group
        best_job = None
        i = 0
        while i < len(unplacedJobsSorted):
            unplacedJobsKV = unplacedJobsSorted[i]
            timeStart = unplacedJobsKV[0]
            jobsAtTime = unplacedJobsKV[1]
            j = 0
            while j < len(jobsAtTime):
                unplacedJob = jobsAtTime[j]
                if (currentEnd is None) or (timeStart > currentEnd):
                    # print(unplacedJob['handlerStart'], unplacedJob['handlerEnd'])
                    currentGroup.append(unplacedJob['jobId'])
                    currentEnd = unplacedJob['handlerEnd']
                    del jobsAtTime[j]
                    continue
                j += 1

            if len(jobsAtTime) == 0:
                del unplacedJobsSorted[i]
                continue
            i += 1

        # print('---')
        if best_job is None:
            groups.append(currentGroup)
            currentGroup = []
            currentEnd = None

    if currentGroup != []:
        groups.append(currentGroup)

    return groups


def findLatestDir(sourceDir):

    def match(filename):
        if os.path.isdir(os.path.join(sourceDir, filename)) is False:
            return False
        if '__' not in os.path.basename(filename):
            return False
        return True

    matchedDirs = [d for d in os.listdir(sourceDir) if match(d)]
    newestDir = max((os.path.getmtime(os.path.join(sourceDir, f)),f) for f in matchedDirs)[1]

    fullPath = os.path.join(sourceDir, newestDir)
    return fullPath


def loadJsonlFile(path):
    rows = []
    with jsonlines.open(path) as reader:
        for row in reader:
            rows.append(row)
    return rows


def getWorkflowName(jobDescriptions):
    name = jobDescriptions[0]['workflowName']
    # additional checks
    for row in jobDescriptions:
        if row['workflowName'] != name:
            raise Exception("Inconsistent jobDescriptions. New workflow name '{}'".format(row['workflowName'])
                + "; last known '{}'.".format(name))
    return name

def getWorkflowSize(jobDescriptions):
    size = jobDescriptions[0]['size']
    # additional checks
    for row in jobDescriptions:
        if row['size'] != size:
            raise Exception("Inconsistent jobDescriptions. New size '{}'".format(row['size'])
                + "; last known '{}'.".format(size))
    return size

def getWorkflowVersion(jobDescriptions):
    version = jobDescriptions[0]['version']
    # additional checks
    for row in jobDescriptions:
        if row['version'] != version:
            raise Exception("Inconsistent jobDescriptions. New version '{}'".format(row['version'])
                + "; last known '{}'.".format(version))
    return version

def getNodeNameForJob(jobId, jobDescriptionsDf):
    name = jobDescriptionsDf.loc[jobDescriptionsDf.jobId==jobId].nodeName.iloc[0]
    return name

def getFirstEventDatetime(metricList):
    minTime = None
    for metric in metricList:
        if metric['parameter'] != 'event':
            continue
        time = metric['time']
        if minTime is None or time < minTime:
            minTime = time
    return minTime

def extractNodesJobs(metricList, jobMap):

    nodesJobsMap = {}
    firstEventTime = getFirstEventDatetime(metricList)

    for metric in metricList:

        if metric['parameter'] != 'event':
            continue

        jobId = metric['jobId']
        nodeName = jobMap[jobId]['nodeName']

        if nodeName not in nodesJobsMap:
            nodesJobsMap[nodeName] = {}

        if jobId not in nodesJobsMap[nodeName]:
            nodesJobsMap[nodeName][jobId] = {}

        eventType = metric['value']
        eventTime = metric['time']
        eventTimeDiff = (eventTime - firstEventTime).total_seconds()

        if eventType in nodesJobsMap[nodeName][jobId]:

            # temproarily just ignore newer handlerStarts
            if (eventType == 'handlerStart') and (eventTimeDiff > nodesJobsMap[nodeName][jobId][eventType]):
                print('WARNING: inconsistent logs - too many handlerStart occurences.')
                continue

            raise Exception('There was already "{}" event for job "{}"'.format(eventType, jobId))

        nodesJobsMap[nodeName][jobId][eventType] = eventTimeDiff

    return nodesJobsMap


def extractNodesJobsNonoverlap(nodesJobs):
    nodesJobsNonoverlap = {}
    for nodeName, jobs in nodesJobs.items():
        # fix sorting for first node name
        if any(char.isdigit() for char in nodeName) is False:
            nodeName += "1"

        jobGroups = splitJobsIntoDisjointGroups(jobs)
        for i, jobGroup in enumerate(jobGroups):
            nodesJobsNonoverlap[nodeName + '_' + str(i)] = jobGroup
    return nodesJobsNonoverlap


def extractOrderedTaskTypes(metricList):
    taskTypes = {}
    for metric in metricList:
        if metric['parameter'] != 'event':
            continue
        name = metric['name']
        metricTime = metric['time']
        if name not in taskTypes:
            taskTypes[name] = metricTime
        elif taskTypes[name] < metricTime:
            taskTypes[name] = metricTime

    orderedTaskTypes = sorted(taskTypes, key=taskTypes.get)
    return orderedTaskTypes

def extractStages(metricList, event_start='jobStart', event_end='jobEnd'):

    firstEventTime = getFirstEventDatetime(metricList)
    stageEvents = defaultdict(list)
    for metric in metricList:

        if metric['parameter'] != 'event':
            continue

        metricTimeOffset = (metric['time'] - firstEventTime).total_seconds()
        eventType = metric['value']
        if eventType == event_start:
            stageEvents[metricTimeOffset].append(1)
        elif eventType == event_end:
            stageEvents[metricTimeOffset].append(-1)

    stages = [{'timeOffset': 0.0, 'activeItems': 0}]
    numActiveItems = 0

    for timeOffset, changes in sorted(stageEvents.items()):
        for change in changes:
            numActiveItems += change
        stages.append({'timeOffset': timeOffset, 'activeItems': numActiveItems})
    return stages

def broken_barh_without_scaling(axis, xranges, yrange, **kwargs):

    # process the unit information
    if len(xranges):
        xdata = cbook.safe_first_element(xranges) # ENTERED
    else:
        xdata = None
    if len(yrange):
        ydata = cbook.safe_first_element(yrange) # ENTERED
    else:
        ydata = None
    axis._process_unit_info(xdata=xdata,
                            ydata=ydata,
                            kwargs=kwargs)
    xranges_conv = []
    for xr in xranges:
        if len(xr) != 2:
            raise ValueError('each range in xrange must be a sequence '
                                'with two elements (i.e. an Nx2 array)')
        # convert the absolute values, not the x and dx...
        x_conv = np.asarray(axis.convert_xunits(xr[0]))
        x1 = axis._convert_dx(xr[1], xr[0], x_conv, axis.convert_xunits)
        xranges_conv.append((x_conv, x1))

    yrange_conv = axis.convert_yunits(yrange)

    col = mcoll.BrokenBarHCollection(xranges_conv, yrange_conv, **kwargs)
    axis.add_collection(col, autolim=False)

    return col

def lightenColor(color, amount=0.5):
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def visualizeDir(sourceDir, displayOnly, showActiveJobs, plotFullNodesNames):
    metricsPath = os.path.join(sourceDir, 'metrics.jsonl')
    metrics = loadJsonlFile(metricsPath)

    jobDescriptionsPath = os.path.join(sourceDir, 'job_descriptions.jsonl')
    jobDescriptions = loadJsonlFile(jobDescriptionsPath)

    jobMap = buildJobMap(jobDescriptions)
    metricList = buildMetricList(metrics)
    workflowName = getWorkflowName(jobDescriptions)
    nodesJobs = extractNodesJobs(metricList, jobMap)
    nodesJobsNO = extractNodesJobsNonoverlap(nodesJobs)
    taskTypes = extractOrderedTaskTypes(metricList)

    # Prepare axis data
    rowHalfHeight = 15
    rowFullHeight = rowHalfHeight * 2
    y_ticks = range(rowHalfHeight, (len(nodesJobsNO)+1)*rowFullHeight, rowFullHeight)
    y_labels = [(key) for key in natsorted(nodesJobsNO.keys())]
    if plotFullNodesNames is False:
        y_labels = map(lambda label: label.rsplit('-', 1)[1], y_labels)
    max_time = 0.0
    for _, jobGroup in nodesJobsNO.items():
        for jobID in jobGroup:
            fullNodeName = jobMap[jobID]['nodeName']
            jobDetails = nodesJobs[fullNodeName][jobID]
            for typex in ['handlerStart', 'jobStart', 'jobEnd', 'handlerEnd']:
                if jobDetails[typex] > max_time:
                    max_time = jobDetails[typex]
    max_time = math.ceil(max_time)

    # Prepare color scheme
    colors = plt.cm.get_cmap("gist_rainbow", len(taskTypes))
    colorsForTaskTypes = {}
    for i, taskType in enumerate(taskTypes):
        colorsForTaskTypes[taskType] = colors(i)

    # Preparing chart background
    plt.rc('figure', figsize=(25,15))
    if showActiveJobs:
        fig, (gnt, gnt2) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
        plt.suptitle(workflowName)
    else:
        fig, gnt = plt.subplots()
        plt.suptitle(workflowName)

    # Plot background color for even lanes
    lastKnownNode = None
    secondColorEnabled = False
    for i, nodeKey in enumerate(natsorted(nodesJobsNO)): # same order as in plot
        jobGroup = nodesJobsNO[nodeKey]
        firstJobID = jobGroup[0]
        fullNodeName = jobMap[firstJobID]['nodeName']
        if fullNodeName != lastKnownNode:
            lastKnownNode = fullNodeName
            secondColorEnabled = not secondColorEnabled
        if secondColorEnabled:
            gnt.axhspan(rowFullHeight*i, rowFullHeight*(i+1), facecolor='grey', alpha=0.2)

    ### SUBPLOT 1

    gnt.title.set_text('Execution process')
    gnt.set_xlim(0, max_time)
    gnt.set_xlabel('Time [s]')
    gnt.set_ylabel('Node_offset')
    gnt.grid(True)
    gnt.set_yticks(y_ticks)
    gnt.set_yticklabels(y_labels)

    # Plotting chart data
    usedLabels = set()
    for i, nodeKey in enumerate(natsorted(nodesJobsNO)):
        jobGroup = nodesJobsNO[nodeKey]
        for jobID in jobGroup:
            job = jobMap[jobID]
            fullNodeName =  job['nodeName']
            jobDetails = nodesJobs[fullNodeName][jobID]
            cColor = colorsForTaskTypes[job['name']]
            cLabel = job['name']
            # prevent duplicated tasks at legend
            if cLabel in usedLabels:
                cLabel = ""
            else:
                usedLabels.add(job['name'])

            broken_barh_without_scaling(gnt, [(jobDetails['jobStart'], jobDetails['jobEnd']-jobDetails['jobStart'])], ((rowHalfHeight+rowFullHeight*i)-8, 16), color=cColor, label=cLabel)
            broken_barh_without_scaling(gnt, [(jobDetails['handlerStart'], jobDetails['handlerEnd']-jobDetails['handlerStart'])], ((rowHalfHeight+rowFullHeight*i)-2, 4), color=lightenColor(cColor,1.3))

    # Draw legend
    handles, labels = gnt.get_legend_handles_labels()
    lOrders = []
    for taskType in taskTypes:
        order = labels.index(taskType)
        lOrders.append(order)
    gnt.legend([handles[idx] for idx in lOrders],[labels[idx] for idx in lOrders], loc='upper center', bbox_to_anchor=(0.5, 1.05),
          ncol=3, fancybox=True, shadow=True)

    ### SUBPLOT 2

    if showActiveJobs:
        gnt2.title.set_text('Active jobs')
        gnt2.set_xlim(0, max_time)
        gnt2.set_xlabel('Time [s]')
        gnt2.set_ylabel('Number of jobs')
        gnt2.grid(True)

        stages = extractStages(metricList)
        stages_x = []
        stages_y = []
        for stage in stages:
            stages_x.append(stage['timeOffset'])
            stages_y.append(stage['activeItems'])
        plt.step(stages_x, stages_y, label='exact value')

        #DEG = 10

        #f = np.poly1d(np.polyfit(np.array(stages_x), np.array(stages_y), DEG))
        #xnew = np.linspace(0, max_time, len(stages)*100)
        #ynew = f(xnew)
        #plt.plot(xnew, ynew, label='approximation ({}-degree)'.format(DEG), ls='--', color='red')

        gnt2.legend(loc="best")

    ### COMMON

    if displayOnly is True:
        plt.show()
    else:
        wfSize = getWorkflowSize(jobDescriptions)
        wfVersion = getWorkflowVersion(jobDescriptions)
        filename = '{}-{}-{}.png'.format(workflowName, wfSize, wfVersion)
        plt.savefig(filename)
        print('Chart saved to {}'.format(filename))
    return


def main():
    parser = argparse.ArgumentParser(description='HyperFlow execution visualizer')
    parser.add_argument('-s', '--source', type=str, required=True, help='Directory with parsed logs')
    parser.add_argument('-d', '--show', action='store_true', default=False, help='Display plot instead of saving to file')
    parser.add_argument('-a', '--show-active-jobs', action='store_true', default=False, help='Display the number of active jobs subplot')
    parser.add_argument('-f', '--full-nodes-names', action='store_true', default=False, help='Display full nodes\' names')
    args = parser.parse_args()
    visualizeDir(args.source, args.show, args.show_active_jobs, args.full_nodes_names)


if __name__ == '__main__':
    main()
