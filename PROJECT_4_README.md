# Project 4: Data Pipeline System Design Patterns 🏛️

**MAANG interview preparation** - Real-world system design problems, architectural patterns, and scalable solutions for data engineering roles at top tech companies.

---

## 📋 Project Overview

Master system design through:
- **Real Interview Questions:** Actual problems asked at Google, Amazon, Microsoft, Meta, Apple
- **Architecture Patterns:** Proven solutions for scale, reliability, cost
- **Trade-off Analysis:** When to use what (consistency vs. availability, batch vs. streaming)
- **Scalability:** Design for 100GB to petabyte-scale datasets
- **Production Readiness:** Monitoring, error handling, disaster recovery

---

## 🎯 Interview Questions Covered

### Question 1: Design a Recommendation Engine

**Scenario:** Netflix wants to provide personalized recommendations for 200M users. Design the data pipeline.

**Constraints:**
- 200M users, 1M videos
- 500M events/day (play, pause, watch time)
- Recommendations must be updated daily
- Latency: <100ms for user requests
- Availability: 99.99% SLA

**Solution Approach:**

```
┌─────────────────────────────────────────────────────────────┐
│ DATA SOURCES                                                │
│ ├─ User events (Kafka: 500M/day)                           │
│ ├─ Video metadata (SQL: 1M records)                        │
│ └─ User interaction history (Data Lake)                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ BATCH LAYER (Daily processing)                              │
│ ├─ Spark job: Process 500M events (8 hours)                │
│ ├─ Collaborative filtering: User-video similarity          │
│ ├─ Feature engineering: Genre, director, rating            │
│ └─ Model training: ML model for ranking                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ SERVING LAYER                                               │
│ ├─ Redis cache: Hot recommendations (1M users)             │
│ ├─ Elasticsearch: Full-text search metadata                │
│ └─ DynamoDB: User preferences, real-time updates           │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ API LAYER                                                   │
│ ├─ Microservices: Personalization, ranking, filtering      │
│ ├─ Load balancing: Traffic distribution                    │
│ └─ CDN: Global content delivery                            │
└─────────────────────────────────────────────────────────────┘
```

**Key Trade-offs:**
- ✅ **Daily batch:** Cost-efficient, accurate models
- ❌ Requires daily waiting for recommendations
- ✅ **Caching:** Sub-100ms latency
- ❌ Memory requirements for 1M user cache

**Scaling Strategy:**
- Partition users by region (Asia, EU, Americas)
- Shard recommendations in Redis by user ID
- Auto-scaling for Spark jobs during peak hours

---

### Question 2: Design an Analytics Data Warehouse

**Scenario:** A Fortune 500 retailer needs to consolidate sales data from 5000 stores worldwide for real-time dashboards.

**Constraints:**
- 5000 stores, 100K SKUs
- 50M transactions/day
- Report generation: <10 seconds
- Data freshness: <1 hour latency
- Storage: <$50K/month

**Solution Architecture:**

```
REAL-TIME INGESTION LAYER
└─ Event streaming
   ├─ Apache Kafka (point-of-sale events)
   ├─ Stream processing (Spark Streaming)
   └─ Mini-batch aggregation (5-min intervals)
         │
         ▼
LAKEHOUSE LAYER (Bronze → Silver → Gold)
├─ Bronze: Raw transactions
├─ Silver: Aggregated metrics (store × SKU × hour)
└─ Gold: Pre-computed reports
         │
         ▼
DATA WAREHOUSE (Azure Synapse / Redshift)
├─ Dimension tables (Store, Product, Date)
├─ Fact tables (Daily sales, inventory)
└─ Pre-aggregated tables (Store totals, regional summaries)
         │
         ▼
CONSUMPTION LAYER
├─ Power BI (interactive dashboards)
├─ APIs (real-time endpoints)
└─ Machine Learning (demand forecasting)
```

**Cost Breakdown ($50K/month):**
- Storage: $10K (200TB at $50/TB/month)
- Compute: $25K (Synapse SQL pool, hourly)
- Kafka/Streaming: $8K
- Data transfer: $7K

**Optimization Techniques:**
- Partition by date (prune old data)
- Compress with Parquet (80% reduction)
- Auto-scale compute (off-peak downscaling)

---

### Question 3: Design Real-Time Fraud Detection

**Scenario:** Stripe needs to detect fraudulent transactions in <100ms for 1M transactions/day.

**Constraints:**
- 1M transactions/day
- <100ms latency requirement
- 99.9% fraud detection accuracy
- Real-time feedback loop

**Architecture:**

```
┌────────────────────────────────────────┐
│ TRANSACTION STREAM (Kafka)             │
│ 1M transactions/day = ~10/second       │
└──────────────┬─────────────────────────┘
               │
               ▼
┌────────────────────────────────────────┐
│ STREAM PROCESSOR (Spark Streaming)     │
│ ├─ Feature extraction: Device, IP,    │
│ │   historical patterns                │
│ ├─ Real-time ML model: XGBoost        │
│ └─ Decision: Approve / Challenge       │
└──────────────┬─────────────────────────┘
               │
        ┌──────┴──────┐
        ▼             ▼
   APPROVED      CHALLENGED
        │             │
        ▼             ▼
   Cache update   User challenge
   (Redis)        (2-factor auth)
```

**ML Model Requirements:**
- Training data: 100M historical transactions
- Features: 50-100 features (device fingerprint, velocity checks, geographic patterns)
- Inference: <50ms per transaction
- Model update: Daily (batch retraining)

**Latency Breakdown:**
- Feature extraction: 10ms
- Model inference: 30ms
- Cache lookup/update: 10ms
- **Total: 50ms (margin for network latency)**

---

### Question 4: Design a Data Warehouse for Ad Analytics

**Scenario:** Google AdWords needs to track billions of impressions, clicks, conversions daily.

**Constraints:**
- 10B+ impressions/day
- 1B clicks/day
- <30 second query latency for reports
- Near real-time updates (5-min latency)

**Data Model:**

```
FACT TABLES (Daily partitions)
├─ Fact_Impressions (4 columns: campaign_id, ad_id, user_id, ts)
├─ Fact_Clicks (4 columns: campaign_id, ad_id, user_id, ts)
└─ Fact_Conversions (4 columns: campaign_id, ad_id, user_id, conversion_value)

DIMENSION TABLES (Slowly changing)
├─ Dim_Campaign (campaign details, budget)
├─ Dim_Ad (creative, bidding strategy)
├─ Dim_User (segmentation, geography)
└─ Dim_Date (day, week, month, quarter)

AGGREGATED TABLES (Pre-computed for speed)
├─ Agg_Daily_Campaign_Performance
├─ Agg_Hourly_Geo_Performance
└─ Agg_User_Segments
```

**Query Example:**

```sql
-- Report: Campaign performance by country
SELECT 
    c.campaign_name,
    d.country,
    COUNT(DISTINCT i.impression_id) as impressions,
    COUNT(DISTINCT cl.click_id) as clicks,
    SUM(CASE WHEN cv.conversion_value > 0 THEN 1 ELSE 0 END) as conversions,
    SUM(cv.conversion_value) as revenue,
    ROUND(100.0 * COUNT(DISTINCT cl.click_id) / COUNT(DISTINCT i.impression_id), 2) as ctr_percent,
    ROUND(1.0 * SUM(cv.conversion_value) / COUNT(DISTINCT cl.click_id), 2) as rpc
FROM fact_impressions i
LEFT JOIN fact_clicks cl ON i.campaign_id = cl.campaign_id AND i.ts = cl.ts
LEFT JOIN fact_conversions cv ON cl.click_id = cv.click_id
JOIN dim_campaign c ON i.campaign_id = c.campaign_id
JOIN dim_geo d ON i.geo_id = d.geo_id
WHERE i.date_key >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY c.campaign_name, d.country
ORDER BY revenue DESC
-- Expected latency: 20 seconds on pre-aggregated tables
```

**Optimization Techniques:**
- Columnar storage (Parquet): 90% compression
- Denormalization for speed: Duplicate date dimension in fact tables
- Pre-aggregation: Store daily/hourly summaries
- Materialized views for common queries

---

### Question 5: Design a Log Analysis Platform

**Scenario:** Uber needs to process 100TB/day of logs for debugging, monitoring, alerting.

**Constraints:**
- 100TB/day of logs
- Query latency: <5 seconds for analytics
- Data retention: 30 days
- Cost optimization priority

**Architecture:**

```
INGESTION (Log shipping from 10K servers)
├─ Fluentd/Logstash (edge collection)
├─ Kafka (buffering, 10+ brokers)
└─ 1000+ topics (one per service)
         │
         ▼
PROCESSING (Stream + Batch)
├─ Stream (15-min micro-batches)
│  ├─ Filter sensitive data
│  ├─ Parse log structure
│  └─ Extract key fields
├─ Batch (daily)
│  ├─ Anomaly detection
│  ├─ Trend analysis
│  └─ Report generation
         │
         ▼
STORAGE LAYER
├─ Hot storage (7 days): ADLS SSD, query-optimized
├─ Warm storage (30 days): ADLS HDD, compressed
├─ Cold storage (archive): Glacier, <$1/TB/month
         │
         ▼
QUERY LAYER
├─ Elasticsearch: Full-text search logs (fast)
├─ Presto/Trino: SQL queries on Parquet (flexible)
└─ Grafana: Time-series visualization (dashboards)
```

**Cost Optimization:**

| Component | Cost | Optimization |
|-----------|------|--------------|
| Storage (100TB/day × 30 days) | $45K | Compress to 20TB = $9K |
| Compute (processing) | $30K | Auto-scaling, spot instances |
| Kafka clusters | $15K | Tiered storage, log cleanup |
| **Total** | **$90K** | **→ $40K with optimizations** |

---

## 📦 Project Structure

```
04-system-design-patterns/
├── README.md
├── INTERVIEW_QUESTIONS.md        # All interview Q&As
├── questions/
│   ├── 01_recommendation_engine/
│   │   ├── PROBLEM.md            # Problem statement
│   │   ├── SOLUTION.md           # Architecture & solution
│   │   ├── TRADEOFFS.md          # Design trade-offs
│   │   ├── IMPLEMENTATION.py     # Sample code
│   │   ├── METRICS.md            # Performance benchmarks
│   │   └── DIAGRAMS.html         # Architecture diagrams
│   │
│   ├── 02_data_warehouse/
│   ├── 03_fraud_detection/
│   ├── 04_ad_analytics/
│   └── 05_log_analytics/
│
├── design_patterns/
│   ├── lambda_architecture/       # Batch + Stream combined
│   ├── kappa_architecture/        # Stream-only
│   ├── medallion_pattern/         # Bronze/Silver/Gold
│   └── dimensional_modeling/      # Fact & Dimension tables
│
├── common_designs/
│   ├── scalable_database.md       # Sharding, partitioning
│   ├── caching_strategies.md      # Redis, Memcached
│   ├── load_balancing.md          # Traffic distribution
│   └── monitoring_alerting.md     # Observability
│
├── implementation_examples/
│   ├── fraud_detection.py         # ML streaming example
│   ├── recommendation_engine.py   # Collaborative filtering
│   ├── log_analysis.py            # Stream processing
│   └── warehouse.py               # ETL patterns
│
├── benchmarks/
│   ├── latency_comparison.md      # Batch vs Stream
│   ├── cost_analysis.md           # Infrastructure costs
│   └── scalability_limits.md      # When to redesign
│
└── interview_prep/
    ├── TALKING_POINTS.md         # How to communicate solutions
    ├── RED_FLAGS.md              # Mistakes to avoid
    ├── FOLLOW_UP_QUESTIONS.md    # Interviewer follow-ups
    └── TIME_MANAGEMENT.md        # 45-min interview strategy
```

---

## 🚀 Interview Preparation Strategy

### Before the Interview (1 week)

```
Day 1-2: Review 5 major design patterns
         └─ Lambda, Kappa, Medallion, DW, Real-time

Day 3-4: Study 5 interview questions in depth
         └─ Recommendation, DW, Fraud, Ads, Logs

Day 5-6: Practice writing architecture diagrams
         └─ Components, data flow, trade-offs

Day 7:   Mock interview with friend/mentor
         └─ 45 minutes, get feedback
```

### During the Interview (45 minutes)

```
0-5 min:   Clarify requirements & constraints
           ├─ Data volume, QPS, latency requirements
           ├─ Availability & consistency needs
           └─ Cost constraints

5-15 min:  High-level architecture
           ├─ Draw major components
           ├─ Data flow between components
           └─ Technology choices (with reasoning)

15-35 min: Deep dive on key components
           ├─ How to handle 1M QPS?
           ├─ Consistency vs. availability trade-off?
           ├─ Cost optimization?
           └─ Scaling strategy?

35-45 min: Follow-up scenarios
           ├─ What if volume increases 10x?
           ├─ How to handle failures?
           └─ Monitoring & alerting?
```

### Speaking Tips

✅ **DO:**
- Ask clarifying questions upfront
- Explain trade-offs clearly
- Draw diagrams as you think
- Ask for feedback mid-interview

❌ **DON'T:**
- Jump to implementation details immediately
- Assume requirements (always ask!)
- Defend a design if alternative is better
- Stay silent (explain your thinking!)

---

## 🎓 Key Design Principles

| Principle | Example | Trade-off |
|-----------|---------|-----------|
| **Scalability** | Sharding, partitioning | Complexity, consistency |
| **Availability** | Replication, failover | Cost, consistency |
| **Consistency** | Transactions, strong semantics | Latency, availability |
| **Cost** | Tiered storage, spot instances | Performance, simplicity |
| **Simplicity** | Fewer components | Scalability, flexibility |

---

## 📚 Study Resources

- **[INTERVIEW_QUESTIONS.md](./INTERVIEW_QUESTIONS.md)** - 10+ real interview Q&As
- **[TALKING_POINTS.md](./interview_prep/TALKING_POINTS.md)** - How to communicate solutions
- **[Design Patterns](./design_patterns/)** - Lambda, Kappa, Medallion, DW patterns
- **Official Resources:** System Design Interview by Alex Xu

---

## 💼 Mock Interview Setup

Want to practice? Here's what to do:

1. **Pick a question** from INTERVIEW_QUESTIONS.md
2. **Set 45-minute timer**
3. **Solve on whiteboard/paper** (no looking at solution)
4. **Record yourself** explaining your design
5. **Review:** Did you cover all components? Trade-offs?

---

## 🎯 Success Metrics

After studying this project, you should be able to:

✅ Design systems for 1M+ QPS  
✅ Explain trade-offs (CAP theorem, batch vs. stream)  
✅ Handle 10x scale increases  
✅ Cost-optimize infrastructure  
✅ Discuss monitoring & reliability  
✅ Answer follow-up questions confidently  

---

## 📞 Questions?

- **Email:** madhureddymandhala@gmail.com
- **GitHub Issues:** [Project Issues](https://github.com/madhureddy809662/Data-Enginnering/issues)

---

*Project Status: Interview-Ready ✅*  
*Last Updated: May 2026*  
*Difficulty: Hard (MAANG level)*
