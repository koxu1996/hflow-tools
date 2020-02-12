# HyperFlow Tools

A collection of small useful tools for the HyperFlow workflow engine

- `hflow-dot`: convert HyperFlow workflow graph to Graphviz dot format. 

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
