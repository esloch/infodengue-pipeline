# Future workload contracts

## BoletimQuinzenal

BoletimQuinzenal is **not integrated**. Its current runtime includes
interactive Google authentication, installation during execution, hardcoded
Drive identifiers, relative sibling data paths, global R state, and rendering
coupled to publication. It cannot yet run as one finite, reproducible worker
job.

Before adding a pipeline bulletin queue, persisted bulletin job, or worker
image, that repository should expose a finite contract conceptually like:

```text
boletim.render-job --week YYYYWW --scope BR|UF --publish false
```

The eventual contract needs an explicit epidemiological week and scope,
noninteractive authentication, dependencies baked into the image, no runtime
`install.packages`, no sibling checkout paths, credentials supplied through
environment or a secret mount, deterministic output location, clear exit
status, finite single-invocation execution, and rendering separated from
publication that is explicitly enabled. The pipeline will not copy bulletin
R code into its repository.

## Analysis completion events

There is no speculative completion event for a hypothetical consumer. When a
real downstream consumer and versioned event contract exist, publish an event
such as `analysis.succeeded` through the existing transactional outbox.

## Multi-host execution

The supported deployment is one Docker host with durable PostgreSQL and
RabbitMQ state and local filesystem analysis artifacts. Output directories
are isolated per analysis job. Multiple `load=false` workers can be supported
on that host; distributing them across hosts requires a shared filesystem or
an object-storage artifact contract first.
