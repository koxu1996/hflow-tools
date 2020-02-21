# HyperFlow Tools

A collection of small useful tools for the HyperFlow workflow engine

- `hflow-info`: print various information about a workflow.
- `hflow-dot`: convert HyperFlow workflow graph to Graphviz dot format. 
- `hflow-metis`: convert HyperFlow workflow graph to Metis format (for graph partitioning).

## Installation
```
npm install -g https://github.com/hyperflow-wms/hflow-tools/archive/master.tar.gz
```

## Usage

### hflow-info
```
hflow-info <workflow.json path>
```

### hflow-dot
```
hflow-dot: converts HyperFlow workflow.json to graphviz dot format
Usage:
  hflow-dot [-p] [--png] <workflow-json-file-path>
  hflow-dot -h|--help
  
Options:
  -h --help   Prints this
  --png       Generate png
  -p          Generates graph according to paritioning info
```

To generate image in another format, use `dot`:
```
dot -Tpdf workflow.dot -o workflow.pdf
```
### hflow-metis
```
hflow-metis: converts HyperFlow workflow.json to Metis graph format
Usage:
  hflow-metis [--ew] [--nw] [--ns] [--lw=<npart>] [--pwgts=<pwgts>] <workflow-json-file-path>
  hflow-metis -h|--help
  
Options:
  -h --help   Prints this
  --ew        Add edge weights (not implemented, probably not needed)
  --nw        Add node weights (requested cpu)
  --ns        Add node size (communication volume)
  --lw=<npart>     Add level weights for 'npart' partitions
  --pwgts=<pwgts>  Partition size weights, e.g. '0.3,0.7'
```
Using [Metis](http://glaros.dtc.umn.edu/gkhome/metis/metis/overview) to generate a partitioning:
```
gpmetis -objtype vol workflow.metis 2
```
Where `vol` is the recommended optimization objective (minimizes communication between partitions), and `2` is the number of partitions.
