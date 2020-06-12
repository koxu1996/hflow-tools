FROM nikolaik/python-nodejs:python3.7-nodejs12 as base
ENV PYROOT /pyroot
ENV PYTHONUSERBASE $PYROOT

FROM base AS builder
RUN pip install 'pipenv==2018.11.26'

WORKDIR /build
COPY hflow-viz-trace/Pipfile* ./
RUN PIP_USER=1 PIP_IGNORE_INSTALLED=1 pipenv install --system --deploy --ignore-pipfile

FROM base
WORKDIR /hflow-tools
COPY --from=builder $PYROOT/lib/ $PYROOT/lib/
COPY . .
RUN npm install -g