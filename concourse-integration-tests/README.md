# concourse-integration-tests
This project contains a collection of integration tests which validate end-user workflows using the real [java driver](../concourse-driver-java) and an embedded instance of [Concourse Server](../concourse-server).

## Usage
_See the [concourse-unit-test-core](../concourse-unit-test-core) project for basic usage._

Each test must extend `ConcourseIntegrationTest` to use an embedded Concourse Server instance and a corresponding client connection from the Java Driver. For each test method, the integration test framework will stop the previously running embedded server, start a new one and establish a client connection for use in the unit tests. 

The embedded server stores data in a temporary directory (usually within a `concourse_<ts>` directory inside the home directory) which is cleaned up after the test finishes.

_NOTE: By default, the embedded Concourse Server uses port **1718**._

### Using the client driver
Each subclass test has access to a protected `client` variable which is a connection to the embedded Concourse Server instance. The client is a fully functional Java Driver, so you can use all the client methods that a user would in application code.
```java
@Test
public void testFoo(){
  client.set("foo", "bar", 1);
  Assert.assertEquals("bar", client.get("foo", 1));
}
```

#### Creating additional connections
If your test requires multiple client connections, you can create them using the standard `Concourse#connect` methods. You can use the protected static `SERVER_HOST` and `SERVER_PORT` variables to ensure that you are connecting to the correct embedded server instance.
```java
@Test
public void testBar(){
  Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin");
}
```

#### Managing credentials
Since the embedded server is newly "installed" for each test, the default credentials (admin/admin) always work. If you want to create additional users or change credentials, you can use the `grantAccess` method within the test or the `beforeEachTest` setup routine.
```java
@Override
public void beforeEachTest(){
  grantAccess("foo", "bar1234!");
  Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "foo", "bar1234!");
}
```

### Controlling the server lifecycle
In general, integration test cases should be focused enough that they don't need to test functionality that requires starting/stopping the embedded server. However, if you want to do this, the framework provides some support.
* You can call `restartServer()` to stop the server and start it again. Any data that existed within the embedded instance will be preserved.
* You can call `reset()` to erase all data from the embedded instance and restart it. In general, you shouldn't need to use this. If you find yourself doing so, it is likely that you should create separate test cases.
