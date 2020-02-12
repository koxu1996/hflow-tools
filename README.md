# HyperFlow Tools

A collection of small useful tools for the HyperFlow workflow engine

- `hflow-dot`: convert HyperFlow workflow graph to Graphviz dot format. 
- `hflow-metis`: convert HyperFlow workflow graph to Metis format (for graph partitioning).

## Installation
```
npm install -g https://github.com/hyperflow-wms/hflow-tools/archive/master.tar.gz
```

## Usage

### hflow-dot
```
hflow-dot [-p] <workflow.json path>
```
Options:
```
-p    generate a partitioning graph (requires 'partitioning' info in workflow.json)
```

To generate an image, use `dot`:
```
dot -Tpng workflow.json.dot -o workflow.png
```
### hflow-metis
```
hflow-metis: converts HyperFlow workflow.json to Metis graph format
Usage:
  hflow-metis [--ew] [--nw] <workflow-json-file-path>
  hflow-metis -h|--help
  
Options:
  -h --help   Prints this
  --ew        Add edge weights (file sizes that need to be transferred)
  --nw        Add node weights (requested cpu)
```
Using [Metis](http://glaros.dtc.umn.edu/gkhome/metis/metis/overview) to generate a partitioning:
```
gpmetis -objtype vol workflow.metis 2
```
Where `vol` is the recommended optimization objective (minimizes communication between partitions), and `2` is the number of partitions.
