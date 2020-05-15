FROM nikolaik/python-nodejs:python3.7-nodejs12
MAINTAINER Mateusz Plinta

ENV USER=docker0

RUN apt-get update && apt-get install sudo

# Create a group and user
RUN addgroup "$USER"
RUN adduser \
    --ingroup "$USER" \
    --disabled-password \
    "$USER"

RUN mkdir -p /etc/sudoers.d
RUN echo "$USER  ALL=NOPASSWD: ALL" > "/etc/sudoers.d/$USER-override"
    
RUN mkdir hflow-tools
COPY . /hflow-tools
RUN chown -R "$USER" /hflow-tools
USER "$USER"
RUN mkdir ~/.npm-global
RUN npm config set prefix '~/.npm-global'
ENV PATH "/home/$USER/.npm-global/bin:$PATH"
RUN npm install -g hflow-tools
