# Install Concourse

!!! note "System Requirements"
    * [JRE or JDK 1.8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
    * At least **256MB** of available system memory
    * Linux or macOS

## Binary Install

1. **Download the installer.** From the terminal, navigate to the location where you want to install Concourse and download the installer for the latest version:

		  curl -o concourse-server.bin -L http://concoursedb.com/download/latest

2. **Run the installer.** Execute the downloaded `.bin` file. You'll be prompted to enter an administrator password so the installer can add the Concourse scripts and log files on your $PATH. This is **recommended** but not required.

		  sh concourse-server.bin

3. **Start Concourse.** Concourse ships with reasonable default configuration so you can use it right out-the-box! If necessary, you can configure how Concourse runs by editing the [concourse.prefs](/) configuration file located in Concourse's [home directory](/).

		  concourse start
