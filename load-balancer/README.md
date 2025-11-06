# ArmoniK Load Balancer

The ArmoniK Load Balancer enables the capability to use multiple ArmoniK clusters from a single endpoint.
It is implemented according to the [AEP 4](https://github.com/aneoconsulting/ArmoniK.Community/blob/main/AEP/aep-00004.md).

When a session is created, a cluster is selected among the configured ones (using a [round-robin scheme](https://en.wikipedia.org/wiki/Round-robin_scheduling)), all tasks of this session will be executed on the selected cluster.
If a cluster becomes unreachable, its sessions are unreachable as well, and their tasks are not executed on another cluster.
Though new sessions will go to the remaining available clusters.

# Usage

Once the load balancer is running, you can redirect your client to the load balancer endpoint.
It does not require any further client modification.

The load balancer does not listen on TLS, if you need this capability, you would need to add an nginx ingress similar to [the one in front of the ArmoniK control plane](https://github.com/aneoconsulting/ArmoniK.Infra/blob/main/armonik/ingress-configmap.tf).

The Admin GUI is not part of the load balancer and should be added in front of it, using the same nginx ingress as previously.

# Configuration

The load balancer can be configured using either a configuration file or some environment variables.

## Configuration file

You can find a complete example of a configuration file in the [repository](lb.example.yaml).

Here is a simplified example:

> `lb.yaml`
> ```yaml
> clusters:
>   remote1:
>     endpoint: https://remote1.example.com:5001/
>   remote2:
>     endpoint: https://remote2.example.com:5001/
> refresh_delay: 60
> ```

The load balancer can then be run with the following command:
```sh
./load-balancer -c lb.yaml
```

Or using the docker image:
```sh
docker run --rm -v "$PWD/lb.yaml:/lb.yaml" dockerhubaneo/armonik_load_balancer -c lb.yaml
```

## Environment variables

You can also pass the configuration using environment variables using (PascalCase)[https://en.wikipedia.org/wiki/Camel_case] separated with `__` and prefixed with `LoadBalancer`:

```sh
export LoadBalancer__Clusters__Remote1__Endpoint="https://remote1.example.com:5001/"
export LoadBalancer__Clusters__Remote2__Endpoint="https://remote2.example.com:5001/"
export LoadBalancer__RefreshDelay="60"
./load-balancer
```

This also works with docker:

```sh
docker run --rm \
  -e LoadBalancer__Clusters__Remote1__Endpoint="https://remote1.example.com:5001/" \
  -e LoadBalancer__Clusters__Remote2__Endpoint="https://remote2.example.com:5001/" \
  -e LoadBalancer__RefreshDelay="60" \
  dockerhubaneo/armonik_load_balancer
```

# Logs

By default, the load balancer logs only errors and warnings to stdout.
If you need more verbose logs, you can configure them using the [`RUST_LOG`](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) environment variable.

Here are some examples:

```sh
# Info for everything
RUST_LOG=info

# Info for everything, debug for armonik and load_balancer
RUST_LOG=info,armonik=debug,load_balancer=debug

# Trace for everything, except gRPC internals
RUST_LOG=trace,h2=off,tower=off
```

# Resource usage

## Memory usage

The Load Balancer memory usage comes from the following factors:
- Number of concurrent connections to the Load Balancer: ~ 20 KB per connection
- Number of concurrent incoming requests (unary or stream): ~ 10 KB per request + size of the request
- Number of concurrent outgoing requests (unary or stream): ~ 120 KB per request + size of the request
- Number of entries in the session, task, result caches: ~ 100 B per entry
- Number of sessions in all the clusters (if the DB is in memory): ~ 1 KB per session

For instance, let's assume we have 3 clusters, each with 200k sesssions, and 5M entries in the caches.
Caches and SQLite database would take `5M * 100B + 3 * 200k * 1KB = 1.1GB`.

Let's now assume we have 10k clients connected simultaneously, each doing 5 `tasks::get` simultaneously repeatedly.
The size of the request and response are small, so are negligible compared to the 10KB of the stream.
The server would take `10k * 20KB + 10k * 5 * 120KB = 7.3GB` in addition to the cache and database usage.

We now assume that our 10k clients are doing 5 `tasks::count_status` simultaneously repeatedly.
The size of the request and response are small, so are negligible compared to the 10KB of the stream.
However, `tasks::count_status` need to query all the downstream clusters instead of only one,
  so each incoming request leads to 3 out-going requests (one per cluster).
The server would take `10k * 20KB + 10k * 5 * 3 * 10KB = 19.3GB` in addition to the cache and database usage.

If we are considering that the 10 clients are doing both previous workloads in parallel,
  the server would take `10k * 20KB + 10k * 5 * 10KB + 10k * 5 * 3 * 10KB = 25.3GB`.
Adding the cache and database memory usage, we arrive at a grand total of `1.1GB + 25.3GB = 26.4GB`.

Of course, the Load Balancer has other memory consumption sources, but for high throughput usage, the rest should be negligible.

Note: The memory usage of an outgoing request is high because it has a dedicated client that could be reused only when the request has been fully processed.

## CPU usage

The CPU usage of the Load Balancer has been measured between 4 and 10 times lower than the CPU usage of the downstream Armonik Control Plane.
Consequently, a rule of thumb for the Load Balancer sizing is to allocate 1 core for every 4 cores of the control plane.
In other words, if each LB instance is configured with the same number of cores as each control plane instance,
  you should have 1 LB instance per 4 control plane instance.

## Connection Limits

Apart from the memory usage of a connection, the Load Balancer is constrained by OS limits.
Especially, the number of connections is limited by the number of opened file descriptors.
The default value on Linux is usually 1024 per process, and can be modified using `ulimit -n`.

If `ulimit` has been raised accordingly and there is enough memory available,
  it is possible to achieve more than 20k connections on a single Load Balancer instance.
If you need to handle more connections, you can spawn multiple instances of the Load Balancer.

## Stream limits

There is no known limitation on the number of opened streams on the Load Balancer, apart from the memory consumption.
It is possible to achieve 100k concurrent streams on a single instance, either from a single connection, or across thousands of connections.
