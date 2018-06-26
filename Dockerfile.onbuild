FROM cinchapi/concourse

# The directory that is included should have a directory called "plugins" which
# contains all the plugins that will be installed when the image is built.
ONBUILD ARG INCLUDE=.

# Copy the INCLUDE directory to the image
ONBUILD COPY ${INCLUDE} /usr/src/include

# Ensure that the expected directories exist, even if empty
ONBUILD RUN mkdir -p /usr/src/include/plugins
ONBUILD RUN mkdir -p /usr/src/include/data

# Install plugins from the INCLUDE directory
ONBUILD RUN concourse start && \
        concourse plugin install /usr/src/include/plugins --password admin \
        concourse stop
