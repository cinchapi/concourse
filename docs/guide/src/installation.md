# Install Concourse

!!! note "System Requirements"
    * [JRE or JDK 1.8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
    * At least **256MB** of available system memory
    * Linux or macOS

## Binary Install

1. **Download the installer.** From the terminal, navigate to the location where you want to install Concourse and download the installer for the latest version:

		  curl -o concourse-server.bin -L http://concoursedb.com/download/latest

2. **Run the installer.** Execute the downloaded `.bin` file. You'll be prompted to enter an administrator password so the installer can add the Concourse scripts and log files to your $PATH. This is **recommended** but not required. *If you don't want this level of system integration, simply press CTRL+C at the prompt.*

		  sh concourse-server.bin

3. **Start Concourse.** Concourse ships with reasonable default configuration so you can use it right out-the-box! If necessary, you can configure how Concourse runs by editing the [conf/concourse.prefs](/) configuration file located in Concourse's [home directory](/).

		  concourse start

## Docker
You can also quickly get started with Concourse using one of our provided [Docker images](https://hub.docker.com/r/cinchapi/concourse/tags/).

### Starting `concourse` with a persistent/shared data directory
```bash
docker run -p 1717:1717 -v </path/to/local/data>:/data --name concourse cinchapi/concourse
```
**NOTE:** The above command will run Concourse in the foreground. To run in the background, add a `-d` flag immediately after the `docker run` command.

### Modifying the configuration
You can add or modifiy any configuration that would normally go in the [`conf/concourse.prefs`](/) file using environment variables that are passed to the docker container. 

Simply UPPERCASE the preference key and prepend it with `CONCOURSE_`. For example, you can modify the `heap_size` preference by passing an environment variable named `CONCOURSE_HEAP_SIZE` to the docker container.
```bash
docker run -p 1717:1717 -e CONCOURSE_HEAP_SIZE=<HEAP_SIZE> -v </path/to/local/data>:/data --name concourse cinchapi/concourse
```

### Using `concourse shell`
You can spin up an ad-hoc docker container running `concourse shell` to connect to the dockerized Concourse instance.
```bash
docker run -it --rm --link concourse:concourse cinchapi/concourse shell --host concourse --password admin
```

### Running client-side CLIs
Similar to `concourse shell` any client-side CLIs (e.g. those that can be run from a remote machine) can be dockerized using an ad-hoc container that is linked to the dockerized Concourse instance.

For example, you can perform an [interactive import](/) that reads input from the command line:
```bash
docker run -it --rm --link concourse:concourse cinchapi/concourse import --host concourse --password admin
```

And, you can similary import a file from the host machine to the dockerized Concourse instance:
```bash
xargs -I % docker run -i --rm --link concourse:concourse --mount type=bind,source=%,target=/data/% cinchapi/concourse import --host concourse --password admin -d /data/% <<< /absolute/path/to/file
```

### Running server-side CLIs
You can run server-side CLIs (e.g. those that must be run from the same machine as Concourse) by leveraging the [`docker exec`](https://docs.docker.com/engine/reference/commandline/exec/) functionality on the container running Concourse.
```bash
docker exec -it concourse concourse <command> <args>
```

For example, you can call the `concourse users sessions` command:
```bash
docker exec -it concourse concourse users sessions --password admin
```

