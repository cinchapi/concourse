FROM openjdk:8
MAINTAINER Cinchapi Inc. <opensource@cinchapi.com>

RUN apt-get update && \
    apt-get -y install sudo

RUN mkdir -p /opt/concourse-server
WORKDIR /opt/concourse-server
COPY . /opt/concourse-server

RUN \
    ./gradlew installer && \
    cp concourse-server/build/distributions/*.bin /opt && \
    cd /opt && \
    sh *bin && \
    cd concourse-server

RUN \
    ln -fsv /dev/stdout ./log/console.log && \
    ln -fsv /dev/stdout ./log/debug.log && \
    ln -fsv /dev/stderr ./log/error.log && \
    ln -fsv /dev/stdout ./log/info.log && \
    ln -fsv /dev/stderr ./log/warn.log

CMD [ "./bin/concourse", "console" ]

EXPOSE 1717
EXPOSE 3434

VOLUME [ "/data/concourse" ]
