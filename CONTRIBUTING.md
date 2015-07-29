# How to contribute
We welcome constructive contributions from everyone, regardless of skill level. Look over this guide for basic information. If you have questions or need additional guidance, send us an email or a private message.

To get started, please <a href="http://www.clahub.com/agreements/cinchapi/concourse">sign the Contributor License Agreement</a> and join the [mailing list](https://groups.google.com/forum/#!forum/concourse-devs) so you can participate in our conversations about planning, etc.

### Learn how things work
1. Introduction to the [data model](http://concoursedb.com/guide/data-model/)
2. Introduction to the [storage model](http://concoursedb.com/guide/storage-model/)
3. Understanding the [codebase](http://concoursedb.com/guide/the-codebase/)
4. Read the [coding standards](https://cinchapi.atlassian.net/wiki/display/CON/Coding+Standards).

### Setup your development environment
1. [Fork](https://github.com/cinchapi/concourse/fork) the repo.
2. Clone your forked version of concourse.git.

		$ git clone git@github.com:<username>/concourse.git
3. Follow the instructions on the [Concourse Dev Setup](https://cinchapi.atlassian.net/wiki/display/CON/Concourse+Dev+Setup) guide.

### Pick an issue
1. Choose an open [issue](https://cinchapi.atlassian.net/issues/?filter=10402) or implement a new feature from the [roadmap](https://cinchapi.atlassian.net/wiki/display/CON/Roadmap). You can also suggest your own projects.
2. [Create an account](https://cinchapi.atlassian.net/secure/Signup!default.jspa) in Jira.
3. If necessary, create a new ticket to track your work. Otherwise, assign an existing ticket to yourself.

### Build your changes
Always check that Concourse builds properly after you've made changes. We use [Gradle](http://www.gradle.org/) as the build system.

	$ ./gradlew clean installer

If all goes well, run the installer in `concourse-server/build/distributions`, launch CaSH and sanity check some [smoke tests](https://cinchapi.atlassian.net/wiki/display/CON/Testing+Zone).

### Submit your changes
1. Send a pull request.
2. Update the Jira ticket with the commit hashes for your change.

### Join the team
Shoot us an [email](mailto:jeff@cinchapi.org) if you want to become a regular contributor and help with strategic planning!

### Report Issues
If you run into any issues, have questions or want to request new features, please create a ticket in [Jira](https://cinchapi.atlassian.net/browse/CON).

### Ask Questions
Ping us at [concourse-devs@cinchapi.org](mailto:concourse-devs@cinchapi.org) if you ever have any questions. We're happy to help.
