# Roadmap

This document outlines planned features and improvements for **spark-lineage-listener**.  
Contributions and feedback are welcome — feel free to open an issue or discussion.

---

## 🚧 In Progress
- [ ] Add support for **DataFrameWriter** APIs (e.g., `insertInto`, `save` with formats)
- [ ] Support raw SQL lineage tracking via `spark.sql`
- [ ] Support additional save methods (e.g., `saveAsTable`)
- [ ] Improve Spark plan → SQL generation
    - Potentially capture the original SQL from Spark
- [ ] Add more lineage transports (starting with [OpenLineage](https://openlineage.io/))

---

## 🔮 Future Ideas
- [ ] Configurable lineage output (JSON, Avro, Protobuf, etc.)
- [ ] Even More transports (Kafka, REST endpoints, file sink)
- [ ] Release a stable version
- [ ] Integration tests
- [ ] Example notebooks / demo pipelines
- [ ] Stress testing
---

## ✅ Done
- [x] Initial release: captures lineage events from SparkListener
- [x] Generates pseudo-SQL alongside lineage metadata
- [x] GitHub Actions setup for build & publish

---

💡 Got an idea? Open a [GitHub Discussion](../../discussions) or [Issue](../../issues).
