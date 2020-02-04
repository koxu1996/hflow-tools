# HyperFlow Dot

`hflow-dot`: small tool to convert HyperFlow workflow graph to Graphviz dot format. 

## Installation
```
npm install -g https://github.com/hyperflow-wms/hflow-dot/archive/master.tar.gz
```

## Usage
```
hflow-dot workflow.json
```

To generate an image, use `dot`:
```
dot -Tpng workflow.json.dot -o workflow.png
```
