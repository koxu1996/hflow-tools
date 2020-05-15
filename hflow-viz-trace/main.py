#!/usr/bin/env python

from natsort import natsorted, ns
import os
import argparse
import jsonlines
import pandas as pd
import datetime
import matplotlib.colors as mc
import colorsys
import math
import matplotlib.pyplot as plt
import numpy as np


def strToDatetime(dateTimeString):
    pattern = '%Y-%m-%dT%H:%M:%S.%f'
    dateTime = datetime.datetime.strptime(dateTimeString, pattern)
    return dateTime


def splitJobsIntoDisjointGroups(jobs):
    groups = []
    unplacedJobs = jobs.copy()
    currentEnd = 0
    currentGroup = []
    while unplacedJobs != []:
        min_ind = None
        min_val = None
        for i, el in enumerate(unplacedJobs):
            if el['handlerStart'] < currentEnd:
                continue
            if (min_ind is None) or (el['handlerStart']< min_val):
                min_ind = i
                min_val = el['handlerStart']
        if min_ind is None:
            groups.append(currentGroup)
            currentGroup = []
            currentEnd = 0
        else:
            best_interval = unplacedJobs[min_ind]
            currentGroup.append(best_interval)
            unplacedJobs.remove(best_interval)
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


def getWorkflowName(jobDescriptionsDf):
    name = jobDescriptionsDf.workflowName[0]
    return name

def getWorkflowSize(jobDescriptionsDf):
    size = jobDescriptionsDf['size'][0]
    return size

def getWorkflowVersion(jobDescriptionsDf):
    version = jobDescriptionsDf.version[0]
    return version

def getNodeNameForJob(jobId, jobDescriptionsDf):
    name = jobDescriptionsDf.loc[jobDescriptionsDf.jobId==jobId].nodeName.iloc[0]
    return name

def getFirstEventDatetime(metricsDf):
    eventsDf = metricsDf.loc[metricsDf.parameter == 'event']
    sortedDf = eventsDf.sort_values(by=['time'])
    firstEventDatetimeString = sortedDf.time.min()

    firstEventDatetime = strToDatetime(firstEventDatetimeString)
    return firstEventDatetime

def extractNodesJobs(metricsDf, jobDescriptionsDf):
    df = metricsDf.sort_values(by=['time'])
    eventsDf = df.loc[df.parameter == 'event']

    firstEventTime = getFirstEventDatetime(metricsDf)

    nodesJobs = {}
    allJobIds = eventsDf.jobId.unique()
    for jobId in allJobIds:

        nodeName = getNodeNameForJob(jobId, jobDescriptionsDf)

        if nodeName not in nodesJobs:
            nodesJobs[nodeName] = []

        jobDf = eventsDf.loc[df.jobId == jobId]
        job = {
            'id': jobId,
            'name': jobDf.iloc[0]['name'],
        }
        for typex in ['handlerStart', 'jobStart', 'jobEnd', 'handlerEnd']:
            stringVal = jobDf.loc[df.value==typex].iloc[0]['time']
            dateTime = strToDatetime(stringVal)
            job[typex] = (dateTime - firstEventTime).total_seconds()
        nodesJobs[nodeName].append(job)

    return nodesJobs


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


def extractTaskTypes(metricsDf):
    taskTypes = metricsDf.loc[metricsDf.parameter == 'event'].loc[metricsDf.value == 'handlerStart'].sort_values(by=['time']).name.unique()
    return taskTypes


def lightenColor(color, amount=0.5):
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def visualizeDir(sourceDir, displayOnly):

    jobDescriptionsPath = os.path.join(sourceDir, 'job_descriptions.jsonl')
    jobDescriptions = loadJsonlFile(jobDescriptionsPath)
    jobDescriptionsDf = pd.DataFrame(jobDescriptions)

    metricsPath = os.path.join(sourceDir, 'metrics.jsonl')
    metrics = loadJsonlFile(metricsPath)
    metricsDf = pd.DataFrame(metrics)

    workflowName = getWorkflowName(jobDescriptionsDf)
    nodesJobs = extractNodesJobs(metricsDf, jobDescriptionsDf)
    nodesJobsNO = extractNodesJobsNonoverlap(nodesJobs)
    taskTypes = extractTaskTypes(metricsDf)

    # Prepare axis data
    rowOffset = 30
    y_ticks = range(rowOffset, (len(nodesJobsNO)+1)*rowOffset, rowOffset)
    y_labels = [(key) for key in natsorted(nodesJobsNO.keys())]
    max_time = 0.0
    for _, jobGroup in nodesJobsNO.items():
        for job in jobGroup:
            for typex in ['handlerStart', 'jobStart', 'jobEnd', 'handlerEnd']:
                if job[typex] > max_time:
                    max_time = job[typex]
    max_time = math.ceil(max_time)

    # Prepare color scheme
    colors = plt.cm.get_cmap("gist_rainbow", len(taskTypes))
    colorsForTaskTypes = {}
    for i, taskType in enumerate(taskTypes):
        colorsForTaskTypes[taskType] = colors(i)

    # Preparing chart background
    plt.rc('figure', figsize=(25,15))
    fig, gnt = plt.subplots()
    plt.title(workflowName)
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
        for job in jobGroup:
            cColor = colorsForTaskTypes[job['name']]
            cLabel = job['name']
            # prevent duplicated tasks at legend
            if cLabel in usedLabels:
                cLabel = ""
            else:
                usedLabels.add(job['name'])
            gnt.broken_barh([(job['jobStart'], job['jobEnd']-job['jobStart'])], (rowOffset*(i+1)-8, 16), color=cColor, label=cLabel)
            gnt.broken_barh([(job['handlerStart'], job['handlerEnd']-job['handlerStart'])], (rowOffset*(i+1)-2, 4), color=lightenColor(cColor,1.3))

    # Draw legend
    handles, labels = gnt.get_legend_handles_labels()
    lOrders = []
    for taskType in list(taskTypes):
        order = labels.index(taskType)
        lOrders.append(order)
    gnt.legend([handles[idx] for idx in lOrders],[labels[idx] for idx in lOrders], loc="best")

    if displayOnly is True:
        plt.show()
    else:
        wfSize = getWorkflowSize(jobDescriptionsDf)
        wfVersion = getWorkflowVersion(jobDescriptionsDf)
        filename = '{}-{}-{}.png'.format(workflowName, wfSize, wfVersion)
        plt.savefig(filename)
        print('Chart saved to {}'.format(filename))
    return


def main():
    parser = argparse.ArgumentParser(description='HyperFlow execution visualizer')
    parser.add_argument('-s', '--source', type=str, required=True, help='Directory with parsed logs')
    parser.add_argument('-d', '--show', action='store_true', default=False, help='Display plot instead of saving to file')
    args = parser.parse_args()
    visualizeDir(args.source, args.show)


if __name__ == '__main__':
    main()

