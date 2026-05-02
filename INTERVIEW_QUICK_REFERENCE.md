# Data Engineering Interview Quick Reference 📚

**Comprehensive cheat sheet for MAANG interviews** - Key concepts, common questions, and answers in quick-reference format.

---

## 🎯 Top 10 Concepts to Master

### 1. **Partitioning vs. Bucketing**

| Aspect | Partitioning | Bucketing |
|--------|--------------|-----------|
| **Purpose** | Prune files during reads | Optimize joins & aggregations |
| **How it works** | Creates subdirectories | Hash-based grouping |
| **Query optimization** | Filter on partition column | Hash-based distributed joins |
| **Example** | `/data/year=2024/month=05/` | Hashes user_id into 100 buckets |
| **When to use** | Large date-based datasets | Many joins on same column |

**Interview Answer:** "Partitioning creates separate directories for each value, allowing Spark to skip files that don't match your filter. Bucketing hashes data into a fixed number of files, which speeds up joins by ensuring data with the same key is in the same bucket."

---

### 2. **CAP Theorem**

**Choose 2 of 3:**
- **Consistency:** All nodes see same data
- **Availability:** System always up
- **Partition tolerance:** System works despite network failures

| System | C | A | P | Example |
|--------|---|---|---|---------|
| **Strong Consistency** | ✓ | ✗ | ✓ | Traditional databases |
| **Eventual Consistency** | ✗ | ✓ | ✓ | NoSQL (DynamoDB, Cassandra) |

**Interview Answer:** "Most distributed systems today choose AP (availability + partition tolerance) with eventual consistency. For data engineering, we typically use strong consistency at write time and eventual consistency for analytics."

---

### 3. **Batch vs. Streaming**

| Factor | Batch | Streaming |
|--------|-------|-----------|
| **Latency** | Hours-to-days | Sub-seconds |
| **Throughput** | Very high (GB/s) | High (MB/s) |
| **Cost** | Lower (bulk processing) | Higher (always-on) |
| **Complexity** | Lower | Higher |
| **Use case** | Daily reports, ML training | Real-time dashboards, fraud detection |

**Interview Answer:** "Use batch for cost-efficiency and simplicity when latency can be hours. Use streaming when you need sub-minute latency. Lambda architecture combines both: streaming for recent data, batch for historical."

---

### 4. **Slowly Changing Dimensions (SCD)**

**Type 1:** Overwrite old values
```sql
UPDATE customers SET email = 'new@email.com' WHERE id = 123
```
**Use:** Non-critical attributes

**Type 2:** Keep history
```sql
-- Add effective_date, end_date, is_current
UPDATE customers SET end_date = TODAY(), is_current = 0 WHERE id = 123
INSERT INTO customers VALUES (123, 'new@email.com', TODAY(), NULL, 1)
```
**Use:** Important attributes (address, job title)

**Type 3:** Keep previous + current
```sql
-- Add previous_value column
UPDATE customers SET previous_email = email, email = 'new@email.com' WHERE id = 123
```
**Use:** Rare - only when history of 1 change matters

**Interview Answer:** "Use SCD Type 2 for dimension tables where history matters. It's the most common because it preserves audit trails without complex logic."

---

### 5. **Shuffle Operations**

**What is shuffling?**
Redistributing data across partitions based on a key (expensive!)

**How to minimize:**
```python
# ❌ CAUSES SHUFFLE: Join and group
df.join(other_df).groupBy("key").sum()

# ✅ BETTER: Broadcast small dataset
df.join(broadcast(small_df)).groupBy("key").sum()

# ✅ BETTER: Filter before join
df.filter(...).join(other_df).groupBy("key").sum()
```

**Interview Answer:** "Shuffles are expensive because they involve network I/O and data movement. Minimize them by broadcasting small tables, filtering early, and pre-sorting data when possible."

---

### 6. **Delta Lake ACID Properties**

**ACID = Atomicity, Consistency, Isolation, Durability**

```python
# Delta Lake guarantees:
# ✓ Atomic: All-or-nothing writes
# ✓ Consistent: No corrupt data
# ✓ Isolated: No dirty reads
# ✓ Durable: Replicated writes

# Enable concurrent writes with ACID guarantees
df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("my_table")

# Time travel (data lineage)
spark.read.option("versionAsOf", 0).table("my_table")  # Read version 0
```

**Interview Answer:** "Delta Lake provides ACID transactions on data lakes. This means you can have concurrent readers/writers without corruption, merge large datasets efficiently, and even time travel to previous versions for auditing."

---

### 7. **Skewed Data Handling**

**Problem:** One partition much larger than others

```python
# ❌ SLOW: Skewed join
orders.join(customers)  # One customer with 1M orders causes bottleneck

# ✅ SOLUTION 1: Salting
from pyspark.sql.functions import rand, concat
orders_salted = orders.withColumn("salt", (rand() * 100).cast("int"))
customers_exploded = customers.crossJoin(
    spark.range(0, 100).select(col("id").alias("salt"))
)
result = orders_salted.join(customers_exploded, ["customer_id", "salt"])

# ✅ SOLUTION 2: Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Auto-optimizes
```

**Interview Answer:** "Skew happens when one value has many more rows than others. Solutions include adding a salt column to randomize distribution, using adaptive query execution, or filtering skewed keys separately."

---

### 8. **Data Quality Framework**

```python
# Define expectations
@dlt.expect("valid_email", "email LIKE '%@%.%'")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect_or_fail("active_status", "status IN ('active', 'inactive')")
def customers():
    return spark.read.table("customers_raw")

# Track metrics
# - Pass rate: % records meeting expectation
# - Failed records: Routed to quarantine zone
# - SLA: Alert if pass rate < 95%
```

**Interview Answer:** "Data quality is about defining expectations and monitoring compliance. Use expectations at ingestion (bronze), validation (silver), and output (gold). Quarantine failed records for investigation."

---

### 9. **Cost Optimization Strategies**

| Strategy | Saving | Trade-off |
|----------|--------|-----------|
| **Spot instances** | 70% cheaper | Can be interrupted |
| **Tiered storage** | Hot/warm/cold storage | Retrieval latency |
| **Compression** | 80% storage reduction | CPU cost |
| **Partitioning** | Avoid full table scans | Query complexity |
| **Data pruning** | Only process needed data | Code complexity |

**Interview Answer:** "Optimize costs by: 1) Partitioning by date/region, 2) Compressing with Parquet, 3) Using tiered storage, 4) Spot instances for non-critical jobs, 5) Materializing common aggregations."

---

### 10. **Monitoring & Alerting**

```python
# Key metrics to track
metrics = {
    "ingestion_lag": "time between data generation and availability",
    "data_freshness": "age of newest data",
    "pipeline_duration": "end-to-end execution time",
    "error_rate": "% of failed records",
    "quality_score": "% records passing expectations",
    "cost_per_query": "infrastructure cost per query"
}

# Alert thresholds
alerts = {
    "high_latency": "if pipeline_duration > 1 hour",
    "low_quality": "if quality_score < 95%",
    "high_errors": "if error_rate > 1%",
    "cost_spike": "if cost_per_query > baseline * 2"
}
```

---

## 🎤 Answering "Walk Me Through Your Architecture"

### Template Answer (5-7 minutes)

```
"I'll cover four layers:

1. INGESTION LAYER
   - Source: [data source]
   - Technology: [Kafka, S3, API]
   - Volume: [10TB/day]
   - Latency: [sub-minute]

2. PROCESSING LAYER
   - Framework: [Spark, DBT, Dataflow]
   - Pattern: [Medallion Architecture]
   - Transformations: [standardization, enrichment]
   - Volume handled: [PB-scale]

3. STORAGE LAYER
   - Format: [Delta Lake, Parquet]
   - Partitioning: [by date, region]
   - Compression: [Snappy, Gzip]
   - Cost: [$X/month]

4. CONSUMPTION LAYER
   - Users: [Analytics team, ML platform]
   - Query time: [<10 seconds]
   - SLA: [99.9% availability]

KEY OPTIMIZATIONS:
- Broadcast joins for dimension tables
- Incremental processing with checkpoints
- Materialized views for common queries
- Auto-scaling for peak hours

TRADE-OFFS:
- Chose batch over streaming for cost
- Using eventual consistency for scale
- Partitioned by date (common query filter)
"
```

---

## ❌ Common Mistakes to Avoid

### Mistake 1: Forgetting to Ask Questions
❌ "I'll design a data warehouse"
✅ "How many users? QPS? Latency requirement? Budget?"

### Mistake 2: Over-Optimizing
❌ "We'll use the latest distributed database"
✅ "We'll use a simple database first, scale when needed"

### Mistake 3: Ignoring Cost
❌ "We'll use premium infrastructure"
✅ "This solution costs $50K/month. Here's the breakdown"

### Mistake 4: No Monitoring Plan
❌ "The pipeline just runs"
✅ "We monitor latency, quality, and cost with alerts"

### Mistake 5: Single Point of Failure
❌ "Data flows through one server"
✅ "We replicate data across regions with failover"

---

## ⏱️ 45-Minute Interview Breakdown

```
0-2 min:   Introduction & question clarification
2-5 min:   Confirm requirements (volume, latency, consistency)
5-20 min:  High-level design (draw components, data flow)
20-35 min: Deep dive on 2-3 key decisions
           - Why this technology?
           - How does it scale to 10x volume?
           - What happens if component fails?
35-43 min: Follow-up questions & trade-offs
43-45 min: Closing remarks & questions for interviewer
```

---

## 📊 Common Data Volumes to Know

| Volume | Duration | Rate |
|--------|----------|------|
| 1TB | 1 month | ~33GB/day |
| 10TB | 1 month | ~333GB/day |
| 100TB | 1 month | ~3.3TB/day |
| 1PB | 1 month | ~33TB/day |

**Latency conversions:**
- 1ms = sub-second (good for real-time)
- 1s = acceptable for real-time
- 1 min = acceptable for near real-time
- 1 hour = batch processing acceptable
- 1 day = overnight batch acceptable

---

## 🔧 Technology Decision Matrix

| Requirement | Technology | Why |
|-------------|-----------|-----|
| **Fast writes** | Kafka, Kinesis | Optimized for throughput |
| **Complex joins** | Spark, Presto | Distributed SQL |
| **ACID transactions** | Delta Lake, Iceberg | Transactional data lakes |
| **Real-time analytics** | Flink, Spark Streaming | Streaming SQL |
| **Data warehouse** | Snowflake, Redshift, Synapse | Optimized for analytics |
| **Time-series** | Prometheus, InfluxDB | Optimized for time-series |
| **Document search** | Elasticsearch | Full-text search |
| **Graph queries** | Neo4j | Relationship queries |

---

## 💬 Handling Tough Questions

### "Your design doesn't handle failures"
✅ "Good catch! Let me add redundancy: we'll replicate data across regions, have a standby cluster, and use automated failover. Here's the updated architecture..."

### "This costs too much"
✅ "You're right. Let me optimize: we can use spot instances (70% cheaper), compress data (80% less storage), and materialize common queries. New cost: $X/month"

### "What if volume increases 100x?"
✅ "We'd need to: 1) Shard the database by region, 2) Add more Kafka brokers, 3) Scale Spark cluster to 100+ nodes. Still viable with cloud auto-scaling."

---

## 🎓 Resources to Study

### Concepts
- [ ] CAP theorem & trade-offs
- [ ] Data modeling (star schema, denormalization)
- [ ] Partitioning strategies
- [ ] Join optimization
- [ ] Window functions
- [ ] Eventual consistency

### Technologies
- [ ] Apache Spark (RDD, DataFrame, SQL)
- [ ] Kafka (topics, partitions, consumer groups)
- [ ] SQL optimization (indexes, execution plans)
- [ ] Delta Lake (ACID, time travel, Z-order)
- [ ] Cloud platforms (AWS, Azure, GCP)

### System Design
- [ ] Lambda architecture (batch + stream)
- [ ] Kappa architecture (stream-only)
- [ ] Medallion architecture (Bronze/Silver/Gold)
- [ ] Event sourcing
- [ ] CQRS pattern

---

## ✅ Pre-Interview Checklist

- [ ] Review this quick reference (30 min)
- [ ] Practice drawing architecture (2-3 times)
- [ ] Explain your past projects (5 times)
- [ ] Prepare for edge cases (failures, scale, cost)
- [ ] Have 3 follow-up questions ready
- [ ] Get good sleep night before
- [ ] Have calm demeanor (explain, don't defend)

---

## 📞 Still Have Questions?

Feel free to reach out:
- **Email:** madhureddymandhala@gmail.com
- **LinkedIn:** [Madhusudan Reddy](https://in.linkedin.com/in/madhusudan-reddy-m/)

---

*Last Updated: May 2026*  
*Use this as a study guide, not a memorization script!*
