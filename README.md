# Music Biometrics System Pipeline

## Pipeline Structure:

```mermaid
graph TD
    classDef source fill:#f9f,stroke:#333,stroke-width:2px;
    classDef ingest fill:#bbf,stroke:#333,stroke-width:2px;
    classDef process fill:#fbb,stroke:#333,stroke-width:2px;
    classDef store fill:#fbf,stroke:#333,stroke-width:2px;
    classDef serving fill:#d7ccc8,stroke:#5d4037,stroke-width:2px;
    classDef view fill:#bfb,stroke:#333,stroke-width:2px;

    subgraph P1 [Phase 1: Data Source Layer]
        A1[Human Biometrics Stream<br>Smartwatch Producer App]:::source
        A2[Human Profile Metadata<br>Demographics & Cultural Context]:::source
        A3[Music Feature Metadata]:::source
    end

    subgraph P2 [Phase 2: Streaming Ingestion & Data Contracts]
        B1[Apache Kafka<br>biometrics-raw Topic]:::ingest
        B2[Confluent Schema Registry<br>Avro Contracts & Compatibility]:::ingest
        B3[Apache Kafka<br>biometrics-dlq Topic]:::ingest

        B4[Apache Kafka<br>user-baseline Compacted Topic]:::ingest
        B5[Apache Kafka<br>realtime-metrics Topic]:::ingest
        B6[Apache Kafka<br>alert-events Topic]:::ingest
    end

    subgraph P3 [Phase 3: Stream & Batch Processing]
        C0[Bootstrap / Batch Metadata Loader<br>Iceberg Writer]:::process

        C1[PySpark Structured Streaming Engine]:::process

        C2[(RocksDB-backed<br>Streaming State Store)]:::process
        C2_CP[(Durable Streaming Checkpoints<br>MinIO / S3)]:::store

        subgraph C3 [Spark Streaming Job Internal Logic]
            C3_1[1. Avro Deserialization<br>& Domain Validation]:::process
            C3_2[2. Watermarking, Deduplication<br>& Event-Time Windowing]:::process
            C3_3[3. Profile and Dynamic<br>Baseline Enrichment]:::process
            C3_4[4. Streaming Feature Engineering<br>& Personalized Stress Deviation Scoring]:::process

            C3_1 --> C3_2
            C3_2 --> C3_3
            C3_3 --> C3_4
        end

        C5[Baseline Publisher Job<br>Publish Latest State by user_id]:::process
    end

    subgraph P4 [Phase 4: Lakehouse Storage & Analytical Processing]

        subgraph Lakehouse_Core [Apache Iceberg Lakehouse Core]
            D1[Apache Iceberg Tables<br>ACID Snapshots - Time Travel - Schema Evolution]:::store
            D1_Cat[Iceberg REST Catalog<br>Namespaces & Atomic Metadata Commits]:::store
            D1_S3[(MinIO Object Storage<br>Data, Manifest & Metadata Files)]:::store

            D1 -.->|Catalog operations| D1_Cat
            D1 -.->|Physical table files| D1_S3
            D1_Cat -.->|Current metadata location| D1_S3
        end

        D3A[DuckDB<br>Embedded dbt Execution Engine]:::store
        D4[dbt<br>SQL Transformation & Data Tests]:::store
    end

    subgraph P5 [Phase 5: Application Serving & Notification]
        E0[ASP.NET Core Minimal API<br>Kafka Consumers - SignalR - Query API]:::serving
        E3[DuckDB.NET<br>Embedded Analytical Query Engine]:::serving
        E1[Real-time Wellness Dashboard]:::view
        E2[Physiological Anomaly Notification<br>Slack - Email - Push]:::view
    end

    %% Static Metadata Bootstrap
    A2 -->|1. Load profile metadata| C0
    A3 -->|2. Load music feature metadata| C0
    C0 -->|3. Create and populate Iceberg tables| D1

    %% Schema Control Plane
    A1 -.->|4a. Register or retrieve Avro schema| B2
    C1 -.->|4b. Retrieve schema for deserialization| B2
    E0 -.->|4c. Retrieve schemas for output topics| B2

    %% Raw Biometrics Ingestion    
    A1 -->|5. Publish Avro events| B1

    A1 -->|5a. Publish producer error envelope<br>when serialization fails| B3

    B1 -->|6. Consume biometric events| C1
    C1 -->|7. Execute streaming pipeline| C3_1

    C3_1 -->|7a. Route malformed or invalid records| B3

    %% Streaming State
    C3_2 <-->|8a. Maintain window and deduplication state| C2
    C3_3 <-->|8b. Maintain latest baseline state by user_id| C2
    C2 -->|8c. Persist recoverable state versions| C2_CP

    %% Context Enrichment
    D1 -->|9a. Read profile and music reference tables| C3_3
    B4 -->|9b. Consume keyed baseline update events| C3_3

    %% Iceberg Streaming Output
    C3_4 -->|10. Append scored biometric events<br>through the Iceberg Spark sink| D1

    %% Real-time Serving Topics
    C3_4 -->|11a. Publish live derived metrics| B5
    C3_4 -->|11b. Publish elevated anomaly events| B6

    B5 -->|12a. Consume live metrics| E0
    B6 -->|12b. Consume notification events| E0

    E0 -->|13a. Push user-scoped SignalR messages| E1
    E0 -->|13b. Dispatch notifications| E2

    %% Batch Analytics and Baseline Updates
    D4 -->|14. Compile models and execute SQL| D3A
    D3A -->|15. Read and write Iceberg analytical tables| D1

    D1 -->|16. Read latest user baseline table| C5
    C5 -->|17. Publish latest baseline keyed by user_id| B4

    %% Historical Query Path
    E1 -->|18. Request historical trends over HTTPS| E0
    E0 -->|19. Execute analytical query| E3
    E3 -->|20. Read Iceberg tables through REST Catalog| D1
    E0 -->|21. Return aggregated trend response| E1
```

## Orchestrator Structure

Airflow orchestrates finite batch workflows only. The long-running Spark Structured Streaming application operates independently of Airflow.

### Orchestration Overview

```mermaid
graph LR
    classDef dag fill:#ffe0b2,stroke:#f57c00,stroke-width:2px
    classDef infra fill:#e1bee7,stroke:#6a1b9a,stroke-width:2px
    classDef kafka fill:#c5cae9,stroke:#283593,stroke-width:2px

    subgraph AF["Apache Airflow"]
        DAG1["System Bootstrap DAG"]:::dag
        DAG2["Music Feature Extraction DAG"]:::dag
        DAG3["Baseline Recalculation and Publish DAG"]:::dag
        DAG4["Iceberg Maintenance DAG"]:::dag
    end

    MINIO["MinIO Object Storage"]:::infra
    CATALOG["Iceberg REST Catalog"]:::infra
    ICEBERG["Apache Iceberg Tables"]:::infra
    SCHEMA["Confluent Schema Registry"]:::kafka
    KBASE["Kafka user-baseline Topic"]:::kafka
    DBT["dbt and DuckDB"]:::infra
    SPARK["Spark Batch and Spark SQL"]:::infra
    AUDIT["Pipeline Audit Table"]:::infra

    DAG1 --> MINIO
    DAG1 --> CATALOG
    DAG1 --> SCHEMA
    DAG1 --> ICEBERG

    DAG2 --> MINIO
    DAG2 --> SPARK
    DAG2 --> ICEBERG
    DAG2 --> AUDIT

    DAG3 --> DBT
    DAG3 --> ICEBERG
    DAG3 --> KBASE
    DAG3 --> AUDIT

    DAG4 --> SPARK
    DAG4 --> CATALOG
    DAG4 --> ICEBERG
    DAG4 --> AUDIT
```

### DAG 1: System Bootstrap

**Schedule:** Manual, one-time initialization.

```mermaid
graph TD
    classDef task fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    classDef quality fill:#dcedc8,stroke:#558b2f,stroke-width:2px

    A["Check Infrastructure Health"]
    B["Create MinIO Buckets and Prefixes"]
    C["Create Iceberg Namespaces and Tables"]
    D["Register Avro Schemas and Compatibility Rules"]
    E["Generate Mock Human Profiles and Initial Baselines"]
    F["Seed Initial Music Metadata"]
    G["Validate Schemas, Row Counts and Null Rules"]:::quality

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
```

Primary interactions:

- MinIO: create buckets and storage prefixes.
- Iceberg REST Catalog: create namespaces and tables.
- Schema Registry: register Avro schemas.
- Iceberg tables: seed profiles, baselines and music metadata.

### DAG 2: Automated Music Feature Extraction

**Schedule:** Periodic polling or event-triggered execution.

```mermaid
graph TD
    classDef task fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    classDef compute fill:#ffcdd2,stroke:#c62828,stroke-width:2px
    classDef quality fill:#dcedc8,stroke:#558b2f,stroke-width:2px
    classDef control fill:#eeeeee,stroke:#616161,stroke-width:2px

    A["List New Audio Objects in MinIO"]
    B{"New Audio Files Found?"}
    C["Create Processing Manifest"]
    D["Extract Audio Features with Librosa"]:::compute
    E["Validate Feature Ranges and Required Fields"]:::quality
    F["Write Valid Features to Iceberg Staging"]
    G["Merge Staging Rows into Music Feature Table"]
    H["Mark Source Objects as Processed"]
    I["Skip Run"]:::control
    J["Write Invalid Results to Rejected Records Table"]:::quality

    A --> B
    B -->|Yes| C
    B -->|No| I
    C --> D
    D --> E
    E -->|Valid| F
    E -->|Invalid| J
    F --> G
    G --> H
```

Idempotency key:

```text
object_key + object_etag + feature_version
```

The processing manifest should store the object key, ETag, processing status, feature version, timestamps and error details.

### DAG 3: Daily Baseline Recalculation and Publication

**Schedule:** Daily, after the previous day's biometric partitions are complete.

```mermaid
graph TD
    classDef task fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    classDef compute fill:#ffcdd2,stroke:#c62828,stroke-width:2px
    classDef quality fill:#dcedc8,stroke:#558b2f,stroke-width:2px

    A["Check Lakehouse and Catalog Health"]
    B["Check Biometric Partitions and Source Freshness"]:::quality
    C["Run dbt Staging Models"]
    D["Recalculate User Baselines"]
    E["Run dbt Data Tests"]:::quality
    F["Validate New Iceberg Baseline Snapshot"]
    G["Read Changed Baseline Rows"]
    H["Publish Updates Keyed by user_id"]:::compute
    I["Verify Kafka Counts, Keys and Schema Version"]:::quality
    J["Record Published Snapshot and Audit Metadata"]

    A --> B
    B --> C
    C --> D
    D --> E
    E -->|Passed| F
    F --> G
    G --> H
    H --> I
    I --> J
```

Data flow:

```text
Iceberg biometric tables
    -> dbt models executed by DuckDB
    -> Iceberg user baseline table
    -> baseline publisher job
    -> Kafka user-baseline compacted topic
    -> Spark Structured Streaming enrichment
```

The Kafka message key must be `user_id` so log compaction retains the latest baseline for each user.

### DAG 4: Iceberg Lakehouse Maintenance

**Schedule:** Daily for high-volume streaming tables and weekly for smaller reference tables.

```mermaid
graph TD
    classDef task fill:#fff3e0,stroke:#ffb74d,stroke-width:1px
    classDef compute fill:#ffcdd2,stroke:#c62828,stroke-width:2px
    classDef quality fill:#dcedc8,stroke:#558b2f,stroke-width:2px
    classDef control fill:#eeeeee,stroke:#616161,stroke-width:2px

    A["Collect File, Manifest and Snapshot Statistics"]
    B{"Maintenance Required?"}
    C["Compact Small Data Files"]:::compute
    D["Rewrite Manifest Files"]:::compute
    E["Expire Old Snapshots"]:::compute
    F["Remove Orphan Files with Safe Retention"]:::compute
    G["Recollect Statistics and Validate Current Snapshot"]:::quality
    H["Skip Maintenance"]:::control

    A --> B
    B -->|Yes| C
    B -->|No| H
    C --> D
    D --> E
    E --> F
    F --> G
```

Maintenance operations are executed through Spark SQL or Spark batch jobs:

```text
rewrite_data_files
rewrite_manifests
expire_snapshots
remove_orphan_files
```

A safe retention window must be used before removing orphan files to avoid deleting files belonging to an active or recently failed writer.


## Backend Structure:

```mermaid
graph LR
    classDef ingest fill:#bbf,stroke:#333,stroke-width:2px;
    classDef process fill:#fbb,stroke:#333,stroke-width:2px;
    classDef serving fill:#d7ccc8,stroke:#5d4037,stroke-width:2px;
    classDef store fill:#fbf,stroke:#333,stroke-width:2px;
    classDef view fill:#bfb,stroke:#333,stroke-width:2px;

    subgraph Input [Kafka Event Bus]
        B5[Kafka Topic<br>realtime-metrics]:::ingest
        B6[Kafka Topic<br>alert-events]:::ingest
        B7[Kafka Topic<br>alert-retry]:::ingest
        SR[Schema Registry<br>Event Contracts]:::ingest
    end

    subgraph Backend [ASP.NET Core Minimal API]

        subgraph Workers [Hosted Background Services]
            W1[RealtimeMetricsConsumer<br>BackgroundService]:::process
            W2[AlertEventsConsumer<br>BackgroundService]:::process
        end

        subgraph Realtime [Real-time Delivery]
            L1[Validate, Map and Filter<br>by user_id]:::process
            HUB[SignalR Hub<br>and IHubContext]:::serving
        end

        subgraph Notification [Notification Delivery]
            L2[Notification Router<br>Idempotency & Retry Policy]:::process
            DISP[External Provider Dispatcher]:::serving
        end

        subgraph QueryAPI [Historical Query API]
            API[Minimal API Endpoint<br>GET /api/users/userId/trends]:::serving
            DNET[DuckDB.NET<br>Embedded Query Engine]:::store
        end

        AUTH[Authentication & Authorization<br>JWT / User Claims]:::serving
    end

    subgraph Lakehouse [Iceberg Lakehouse]
        CAT[Iceberg REST Catalog]:::store
        S3[(MinIO<br>Iceberg Data & Metadata Files)]:::store
    end

    subgraph Output [Application Sinks]
        UI[Web Wellness Dashboard]:::view
        ALERTS[Slack - Email - Push Provider]:::view
    end

    B5 -->|1. Contracted metric events| W1
    W1 -.->|Retrieve event schema| SR
    W1 -->|2. Deserialize and validate| L1
    L1 -->|3. Route by authenticated user_id| HUB
    HUB -->|4. SignalR message| UI

    B6 -->|5. Contracted anomaly events| W2
    B7 -->|6. Retry previously failed delivery| W2
    W2 -.->|Retrieve event schema| SR
    W2 -->|7. Validate and deduplicate by event_id| L2
    L2 -->|8. Select notification channel| DISP
    DISP -->|9. HTTP API / SMTP| ALERTS
    DISP -->|10. Transient delivery failure| B7

    UI -->|11. HTTPS historical trend request| API
    API -->|12. Execute parameterized analytical query| DNET
    DNET -->|13a. Resolve table metadata| CAT
    DNET -->|13b. Read Iceberg files| S3
    API -->|14. Return aggregated response| UI

    AUTH -.->|Authorize SignalR connection| HUB
    AUTH -.->|Authorize REST endpoint| API
```