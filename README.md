Daisy is an open-source column-oriented and streaming time series database management system built on top of ClickHouse. At it core, it leverages the super power of single instance ClickHouse but provides almost infinite horizontal scalability in data ingestion and query in distributed mode. Daisy combines the two best worlds of real-time streaming processing and data warehouse which we called `Streaming Warehouse`. Internally, it is tailored for time series data and `time` is the first class concept in Daisy.

# Achitecture

In distributed mode, Daisy depends on a high throughput and low latency distributed write ahead log for data ingestion and streaming query. Also it depends on this distributed write ahead log for metadata management and node membership management etc tasks. This architecture is inspired by lots of prior work in academic and industry, to name a few [LinkedIn Databus](https://dl.acm.org/doi/10.1145/2391229.2391247), [SLOG](http://www.vldb.org/pvldb/vol12/p1747-ren.pdf), and lots of work from [Martin Kleppmann](https://martin.kleppmann.com/)

achitecture highlights

1. Extremely high performant data ingestion and query. Every node in the cluster can serve ingest and query request.
2. Time as first class concept and internally the engine is optimized for it
3. Built-in streaming query capability
4. At least once delivery semantic by default. Can elimnitate data duplication in practical scenarios in distributed mode
5. Automatic data shard placement
6. Mininum operation overhead if running in K8S

![Daisy Architecture](https://github.com/datatlas-io/daisy/raw/develop/design/daisy-high-level-arch.png)
