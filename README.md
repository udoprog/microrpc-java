# MicroRPC

An efficient RPC framework built on top of Netty.

* Supports connection pooling and retrying of requests.
* Uses [TinySerializer](https://github.com/udoprog/tiny-serializer-java) for efficient serialization completely under your control
* Uses [TinyAsync](https://github.com/udoprog/tiny-async-java) for expressive asynchronous operations, which adds transparency from the client to the server.

#Setup

Add microrpc as a dependency to your project.

```
<dependency>
  <groupId>eu.toolchain.microrpc</groupId>
  <artifactId>microrpc</artifactId>
  <version>CHANGEME</version>
</dependency>
```

# Usage

MicroRPC uses an instance of ```MicroServer``` to maintain both outgoing connections, and a local server.

Connections are established by calling ```MicroServer#connect(URI)```, which returns a ```MicroConnection``` instance.

This instance will be usable until you call ```MicroConnection#close()```.

Requests are sent using ```AsyncFuture<T> MicroConnection#request(String, Serializer<T>, ...)``` methods, which expect you to provide a serializer for the request body, and the result body.

The returned ```AsyncFuture``` will be resolved, failed, or cancelled at the frameworks discretion and you should take appropriate action to handle these situations.

For a complete example on how to use this framework, see [Server and client example](src/example/java/eu/toolchain/examples/ClientAndServerExample.java)