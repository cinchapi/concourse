# Copyright (c) 2018 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8
MAINTAINER Cinchapi Inc. <opensource@cinchapi.com>

# Install sudo because some of the Concourse scripts require it
RUN apt-get update && \
    apt-get -y install sudo

# Install Ruby to generate CaSH docs
RUN apt-get -y install ruby-full

# Copy the application source to the container
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app

# Build the installer
RUN \
    ./gradlew installer && \
    mkdir -p /opt && \
    cp concourse-server/build/distributions/*.bin /opt

# Install the app
WORKDIR /opt
RUN sh *bin

# Link log files
WORKDIR concourse-server
RUN \
    ln -fsv /dev/stdout ./log/console.log && \
    ln -fsv /dev/stdout ./log/debug.log && \
    ln -fsv /dev/stderr ./log/error.log && \
    ln -fsv /dev/stdout ./log/info.log && \
    ln -fsv /dev/stderr ./log/warn.log

# Link the default data directory to the persistent volume
RUN ln -s /root/concourse /data

# Create a persistent volume for data
VOLUME [ "/data" ]

# Configuration
ENV CONCOURSE_BUFFER_DIRECTORY /data/buffer
ENV CONCOURSE_DATABASE_DIRECTORY /data/db

# Start the app
ENTRYPOINT [ "concourse"]
CMD ["console"]

# Expose the TCP and HTTP ports
EXPOSE 1717
EXPOSE 3434


