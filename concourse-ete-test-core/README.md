Concourse ETE Test Framework
============================

The concourse-ete-test framework extends JUnit and allows Java developers to write realistic end-to-end tests that are backed by an external Concourse Server instance that is fully managed by the framework.

## Usage
The major benefit of this framework is that it equips each test method with a fully functional Concourse Server instance that is lives in a separate JVM, but is completely managed by the framework. Additionally, lots of bolierplate is taken care of so developers can test code against version of Concourse with confidence.

### General Workflow
The test framework automatically downloads (if necessary), installs and starts a new Concourse Server instance for each test method. The data for the instance is stored in a temporary location and is automatically cleaned up at the end of the test.

#### Using the client
Each test that uses the framework has access to a `client` variable. This is a fully functional client connection that uses the Java Driver associated with the version of the Concourse Server instance that is provided for the test.

#### Managing the server
Each test that uses the framework has access to a `server` variable that enables arbitrary management of the server process that is running in a separate JVM.
* The `connect` methods allow you to easily create a new client connection using the default credentials or provided ones.
* The `destroy` method will stop the server and permanently delete the application files and any associated data.

### Single version tests
To run a test case against a single specific version of the Concourse, extend the `ClientServerTest` base class. Doing so ensures that the test is equipped with a temporary server instance against which it can issue test actions using the `client` API. The temporary server is completely managed by the test framework but lives in a separate JVM process, which means that test cases run in a realistic environment.

#### Specify the version
Each test class must implement the `getServerVersion` method and return a string that describes the release version number against which the tests should run (formatted as a `<major>.<minor>.<patch>` version number) OR the path to a local server installer (i.e. a `.bin` file).

### Cross versions tests
Sometimes you'll want to run a single test case against multiple versions at the same time in order to compare performance and functionality. To do so, extend the `CrossVersionTest` base class. Doing so ensures that your tests are controlled by a custom runner that executes each test against all the specified versions. Just like when running a sngle version test, you have access to the `client` API which iteracts with the temporary servers for each version in separate JVMs.

#### Speicfy the versions
For a `CrossVersionTest` you will need to specify all of the release versions (formatted as a `<major>.<minor>.<patch>` version number) and/or paths to local installers using the `Versions` annotation.

### Setup and teardown
The framework automatically ensures that each test is equipped with a fully functional temporary server that is torn down at the end. But, each test class can also define additional setup and tear down methods in the `beforeEachTest` and `afterEachTest` methods.

### Debugging
The framework provides each test class with a global `Variables` register which will dump the state of the test case whenever it fails. To take advantage of this feature, simply register all of the interesting varaiables in your test implementation:

	@Test
	public void testAdd(){
		String key = Variables.register("key", "count");
		Object value = Variables.register("value", 1);
		long record = Variables.register("record", 1);
		...
	}

## General Information

### Versioning

This is version 0.5.0 of the concourse-test-framework.

This project will be maintained under the [Semantic Versioning](http://semver.org)
guidelines such that release versions will be formatted as `<major>.<minor>.<patch>`
where

* breaking backward compatibility bumps the major,
* new additions while maintaining backward compatibility bumps the minor, and
* bug fixes or miscellaneous changes bumps the patch.
