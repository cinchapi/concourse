FROM openjdk:8
MAINTAINER Cinchapi Inc. <opensource@cinchapi.com>

RUN \
    ./gradlew installer
    cp concourse-server/build/distributions/*.bin /opt && \
    cd /opt && \
    sh *bin && \
    cd concourse-server

WORKDIR /opt/concourse-server

RUN \
    ln -fsv /dev/stdout ./log/console.log && \
    ln -fsv /dev/stdout ./log/debug.log && \
    ln -fsv /dev/stderr ./log/error.log && \
    ln -fsv /dev/stdout ./log/info.log && \
    ln -fsv /dev/stderr ./log/warn.log

CMD [ "./bin/concourse", "console" ]

EXPOSE 1717 8817

VOLUME [ "/data/concourse" ]
