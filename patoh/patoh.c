#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include "patoh.h"


#define WDIFF         10


#define umax(a, b)    (((a) >= (b)) ? (a) : (b))

#define usRandom(s)     srand(s)
#define uRandom(l)      (rand() % l)

void usage(char *name) {
    printf("usage: %s <hypergraph-filename> <#parts> <partition-weights-filename> <fixed-cells-filename>\n", name);
    printf("\t<partition-weights-filename> format:\n");
    printf("\t\t<partition-number> = <partition-weight>\texample: 0 = 0.3\n");
    printf("\t<fixed-cells-filename> format:\n");
    printf("\t\t<vertex-number> = <fixed-partition>\texample: 1 = 2\n");
    exit(1);
}

void PrintInfo(char *pname, int _k, int *partweights, int cut, int _nconst, float *targetweights) {
    double *tot, *maxi;
    int i, j;


    tot = (double *) malloc(sizeof(double) * _nconst);
    maxi = (double *) malloc(sizeof(double) * _nconst);

    /* normalize target weight sum to 1.0 */
    for (j = 0; j < _nconst; ++j)
        tot[j] = 0.0;
    for (i = 0; i < _k; ++i)
        for (j = 0; j < _nconst; ++j)
            tot[j] += targetweights[i * _nconst + j];
    for (i = 0; i < _k; ++i)
        for (j = 0; j < _nconst; ++j)
            targetweights[i * _nconst + j] /= tot[j];

    printf("\n-------------------------------------------------------------------");
    printf("\n Partitioner: %s", pname);

    printf("\n %d-way cutsize = %d \n", _k, cut);

    printf("\nPart Weights are:\n");
    for (i = 0; i < _nconst; ++i)
        maxi[i] = tot[i] = 0.0;
    for (i = 0; i < _k; ++i)
        for (j = 0; j < _nconst; ++j)
            tot[j] += partweights[i * _nconst + j];
    for (i = 0; i < _nconst; ++i)
        maxi[i] = 0.0;

    for (i = 0; i < _k; ++i) {
        printf("\n %3d :", i);
        for (j = 0; j < _nconst; ++j) {
            double im = (double) ((double) partweights[i * _nconst + j] - tot[j] * targetweights[i * _nconst + j]) /
                        (tot[j] * targetweights[i * _nconst + j]);

            maxi[j] = umax(maxi[j], im);
            printf("\n%10d (TrgtR:%.3f  imbal:%6.3lf%%) ", partweights[i * _nconst + j], targetweights[i * _nconst + j],
                   100.0 * im);
        }
    }

    printf("\n MaxImbals are:");
    printf("\n      ");
    for (i = 0; i < _nconst; ++i)
        printf("%10.1lf%% ", 100.0 * maxi[i]);
    printf("\n");
    free(maxi);
    free(tot);
}


int main(int argc, char *argv[]) {
    PaToH_Parameters args;
    int nrOfCells, nrOfNets, nrOfConstraints, *cwghts, *netsCosts,
            *xpins, *pins, *fixedCells, cutSizeOut, *constraintPartitionWeightsOut,
            i, j;
    float *targetWeights, *skewedWeights;

    if (argc < 4) {
        usage(argv[0]);
    }

    usRandom(time(NULL));

    PaToH_Read_Hypergraph(argv[1], &nrOfCells, &nrOfNets, &nrOfConstraints, &cwghts,
                          &netsCosts, &xpins, &pins);

    printf("===============================================================================\n");

    printf("Hypergraph %10s -- #Cells=%6d  #Nets=%6d  #Pins=%8d #Const=%2d\n",
           argv[1], nrOfCells, nrOfNets, xpins[nrOfNets], nrOfConstraints);

    PaToH_Initialize_Parameters(&args, PATOH_CONPART,
                                PATOH_SUGPARAM_DEFAULT);

    args._k = atoi(argv[2]);
    args._k = (args._k > 2) ? args._k : 2;
    args.seed = 1;

    fixedCells = (int *) malloc(nrOfCells * sizeof(int));
    constraintPartitionWeightsOut = (int *) malloc(nrOfConstraints * args._k * sizeof(int));
    targetWeights = (float *) malloc(args._k * nrOfConstraints * sizeof(float));
    skewedWeights = (float *) malloc(args._k * nrOfConstraints * sizeof(float));

    PaToH_Alloc(&args, nrOfCells, nrOfNets, nrOfCells, cwghts, netsCosts,
                xpins, pins);

    char buf[120];
    FILE *fp;
    // SET PREASSIGNED CELLS
    int useFixedCells = 0;
    if (argc > 4) {
        useFixedCells = 1;
        memset(fixedCells, 0xff, nrOfCells * sizeof(int));
        fp = fopen(argv[4], "r");
        while (fgets(buf, sizeof buf, fp) != NULL) {
            char *ptr = strtok(buf, " = ");
            int vertexNum = atoi(ptr);
            int partitionNum = atoi(strtok(NULL, " = "));
            fixedCells[vertexNum] = partitionNum;
        }
        fclose(fp);
    }

    fp = fopen(argv[3], "r");
    while (fgets(buf, sizeof buf, fp) != NULL) {
        char *ptr = strtok(buf, " = ");
        int partNum = atoi(ptr);
        float size = atof(strtok(NULL, " = "));
        targetWeights[partNum] = size;
    }
    fclose(fp);

    for (i = 0; i < args._k * nrOfConstraints; ++i) {
        skewedWeights[i] = targetWeights[i / nrOfConstraints];
    }

    PaToH_Part(
            &args, // parameters structure
            nrOfCells,
            nrOfNets,
            nrOfConstraints,
            useFixedCells,
            cwghts, // should be of size nrOfNets*nrOfConstraints
            netsCosts, // should be of size nrOfNets
            xpins, // array of size nrOfNets+1 that stores the beginning index of pins (cells) connected to nets.
            pins, // array that stores the pin-lists of nets. Cells connected to net nj are stored in pins[xpins[j]] through pins[xpins[j+1]-1].
            skewedWeights, // of size args->k, where k is number of partitions
            fixedCells, // of size nrOfCells, store fixed partition number // OUT
            constraintPartitionWeightsOut, //  total part weight of each part.
            &cutSizeOut
    );
    PrintInfo("MultiConstraint FixedCells", args._k, constraintPartitionWeightsOut, cutSizeOut, nrOfConstraints,
              skewedWeights);

    printf("\n");


    fp = fopen("patoh_partition_file.part", "w");

    for (int i = 0; i < nrOfCells; i++) {
        if (i == nrOfCells - 1) {
            sprintf(buf, "%d", fixedCells[i]);
        } else {
            sprintf(buf, "%d\n", fixedCells[i]);
        }
        fputs(buf, fp);
    }

    return 0;
}