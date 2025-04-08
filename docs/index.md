<div style="display: flex; align-items: center; gap: 20px; margin: 10px;">

  <img src="./img/compodb.png" alt="CompoDB Logo" width="90"/>

  <h1 style="margin: 0 0 15px 0;">CompoDB: A Composable Data Management System</h1>

</div>


This is CompoDB, a framework for composing modular DMSes
using standardized interfaces for query parsers, optimizers, and
execution engines to enable a seamless assembly and evaluation
of modular DMSes. It includes built-in benchmarking functionality 
to evaluate the performance of DMS compositions by
systematically measuring trade-offs across configurations, including 
runtime differences and intermediate query plans. With these capabilities, 
CompoDB establishes a foundation for modular “plug &
play” DMS design to potentially select the optimal combination of components 
based on any given workload.


## Sections

- [CompoDB](./compodb/architecture.md)
  - [CompoDB Architecture](./compodb/architecture.md)
  - [Components](./compodb/components.md)
  - [Procedures](./compodb/procedure.md)
- [Benchmark Framework](./benchmark-demo/benchmarks.md)
  - [Benchmark Suite](./benchmark-demo/benchmarks.md)
  - [Demo-UI](./benchmark-demo/demoUI.md)
  - [Outcomes](./benchmark-demo/outcomes.md)
- [Quickstart](./quickstart.md)