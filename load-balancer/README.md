# ArmoniK Load Balancer

The ArmoniK Load Balancer enables to target multiple ArmoniK clusters from a single endpoint.
It is implemented according to the [AEP 4](https://github.com/aneoconsulting/ArmoniK.Community/blob/main/AEP/aep-00004.md).

When a session is created, a cluster is selected among the configured ones (using a round-robin scheme), and all tasks of this session will be executed on the selected cluster.
If a cluster becomes unreachable, its sessions are unreachable as well, and their tasks are not executed on another cluster.
New sessions will go to the remaining available clusters, though.

# Usage

Once the load balancer is running, you can redirect your client to the load balancer endpoint, and it does not require any further client modification.

The load balancer does not listen using TLS, so if you need this capability, you would need to add an nginx ingress like [the one in front of the ArmoniK control plane](https://github.com/aneoconsulting/ArmoniK.Infra/blob/main/armonik/ingress-configmap.tf).

The Admin GUI is not part of the load balancer and should be added in front of it, using the same nginx ingress as previously.

# Configuration

The load balancer can be configured using either a configuration file, or environment variables.

## Confiuration file

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

You can also pass the configuration using environment variables using PascalCase separated with `__` and prefixed with `LoadBalancer`:

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
RUST_LOG=info,armonik=debug,load-balancer=debug

# Trace for everything, except gRPC internals
RUST_LOG=trace,h2=off,tower=off
```
