# Database Sync Service - Deployment Guide

## Overview

This service performs one-way batched synchronization of new rows between two PostgreSQL databases with identical schemas. It's designed for security scanning workflows where findings and assets need to be synchronized from a source database to a target database.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Development Setup](#development-setup)
- [Production Deployment](#production-deployment)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Security Considerations](#security-considerations)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Access to source and target PostgreSQL databases
- `config.json` file with table synchronization configuration

### Local Development

1. **Clone and prepare configuration:**
```bash
git clone <repository>
cd "Scan sync"
cp config.json.example config.json  # Edit as needed
```

2. **Run with test databases:**
```bash
docker-compose up --build
```

3. **Run with existing databases:**
```bash
docker build -t syncsvc .
docker run --rm \
  -e SOURCE_DSN="postgres://scoutUser:scoutPass@localhost:5433/scout?sslmode\=disable" \
  -e TARGET_DSN="postgres://scoutUser:scoutPass@localhost:5433/scanner?sslmode\=disable" \
  -v ./config.json:/root/config.json:ro \
  syncsvc
  
  docker run --rm \
    -e SOURCE_DSN="postgres://scoutUser:scoutPass@localhost:5433/scout?sslmode=disable" \
    -e TARGET_DSN="postgres://scoutUser:scoutPass@localhost:5433/scanner?sslmode=disable" \
    -v "/home/bogdan/Projects/Scan sync/config.json":/root/config.json:ro \
    syncsvc
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOURCE_DSN` | Yes | - | PostgreSQL connection string for source database |
| `TARGET_DSN` | Yes | - | PostgreSQL connection string for target database |
| `CONFIG_PATH` | No | `./config.json` | Path to table configuration file |
| `BATCH_SIZE` | No | `1000` | Number of rows to process per batch |
| `DRY_RUN` | No | `false` | Set to `true` to simulate without writing to target |

### Connection String Format

```
postgres://username:password@hostname:port/database?sslmode=require
```

**SSL Modes:**
- `disable` - No SSL (development only)
- `require` - SSL required (production)
- `verify-full` - SSL with certificate verification (recommended)

### Table Configuration (`config.json`)

```json
{
  "tables": [
    {
      "name": "public.findings",
      "key_columns": ["asset_id", "vulnerability_id"],
      "sync_column": "created_at"
    },
    {
      "name": "public.assets",
      "key_columns": ["id"],
      "sync_column": "updated_at"
    }
  ]
}
```

**Configuration Fields:**
- `name`: Full table name with schema (e.g., `public.table_name`)
- `key_columns`: Columns forming unique constraint in target database
- `sync_column`: Timestamp column for incremental sync (must be monotonic)

## Development Setup

### Using Docker Compose

The provided `docker-compose.yml` includes:
- Source PostgreSQL database (`scout`) on port 5433
- Target PostgreSQL database (`scanner`) on port 5434
- Sync service with health checks

```bash
# Start databases only
docker-compose up postgres-source postgres-target

# Run sync service
docker-compose up syncsvc

# Clean up
docker-compose down -v
```

### Native Go Development

```bash
# Install dependencies
go mod download

# Build
go build -o syncsvc

# Run with environment variables
export SOURCE_DSN="postgres://scoutUser:scoutPass@localhost:5433/scout?sslmode=disable"
export TARGET_DSN="postgres://scoutUser:scoutPass@localhost:5434/scanner?sslmode=disable"
export DRY_RUN="true"
./syncsvc
```

## Production Deployment

### 1. Docker Deployment

#### Standalone Container

```bash
# Build production image
docker build -t syncsvc:v1.0.0 .

# Run in production
docker run --rm \
  --name syncsvc \
  -e SOURCE_DSN="postgres://sync_user:$SYNC_PASSWORD@source-db.internal:5432/production?sslmode=require" \
  -e TARGET_DSN="postgres://sync_user:$SYNC_PASSWORD@target-db.internal:5432/analytics?sslmode=require" \
  -e BATCH_SIZE="5000" \
  -v /path/to/production-config.json:/root/config.json:ro \
  --network production \
  syncsvc:v1.0.0
```

#### Docker Compose for Production

```yaml
version: '3.8'

services:
  syncsvc:
    image: syncsvc:v1.0.0
    environment:
      SOURCE_DSN: "postgres://sync_user:${SYNC_PASSWORD}@source-db:5432/production?sslmode=require"
      TARGET_DSN: "postgres://sync_user:${SYNC_PASSWORD}@target-db:5432/analytics?sslmode=require"
      BATCH_SIZE: "5000"
    volumes:
      - ./production-config.json:/root/config.json:ro
    networks:
      - production-network
    restart: "no"
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

networks:
  production-network:
    external: true
```

### 2. Kubernetes Deployment

#### ConfigMap for configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: syncsvc-config
data:
  config.json: |
    {
      "tables": [
        {
          "name": "public.findings",
          "key_columns": ["asset_id", "vulnerability_id"],
          "sync_column": "created_at"
        }
      ]
    }
```

#### Job Definition

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: database-sync
spec:
  template:
    spec:
      containers:
      - name: syncsvc
        image: syncsvc:v1.0.0
        env:
        - name: SOURCE_DSN
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: source-dsn
        - name: TARGET_DSN
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: target-dsn
        - name: BATCH_SIZE
          value: "5000"
        volumeMounts:
        - name: config
          mountPath: /root/config.json
          subPath: config.json
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
      volumes:
      - name: config
        configMap:
          name: syncsvc-config
      restartPolicy: Never
  backoffLimit: 3
```

#### CronJob for Scheduled Sync

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: database-sync-cron
spec:
  schedule: "0 */2 * * *"  # Every 2 hours
  jobTemplate:
    spec:
      template:
        spec:
          # Same as Job template above
          restartPolicy: Never
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
```

### 3. Cron-based Deployment

```bash
# Add to crontab (every 15 minutes)
*/15 * * * * /usr/local/bin/docker run --rm \
  -e SOURCE_DSN="$SOURCE_DSN" \
  -e TARGET_DSN="$TARGET_DSN" \
  -v /etc/syncsvc/config.json:/root/config.json:ro \
  syncsvc:v1.0.0 >> /var/log/syncsvc.log 2>&1
```

## Monitoring & Troubleshooting

### Health Checks

The service logs its progress and can be monitored through:

```bash
# Docker logs
docker logs <container_id>

# Follow logs in real-time
docker logs -f <container_id>

# Kubernetes logs
kubectl logs job/database-sync
```

### Common Issues

#### 1. DNS Resolution Error
```
hostname resolving error: lookup host
```
**Solution:** Ensure correct hostname in DSN or use IP address.

#### 2. No Columns Discovered
```
no columns discovered for public.table_name
```
**Solutions:**
- Verify table exists: `\dt public.table_name`
- Check schema name in config.json
- Ensure database user has SELECT permissions

#### 3. Connection Timeout
```
connection timeout
```
**Solutions:**
- Check network connectivity
- Verify database is running and accepting connections
- Increase connection timeout in DSN: `?connect_timeout=30`

#### 4. Permission Denied
```
permission denied for table
```
**Solution:** Grant necessary permissions:
```sql
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sync_user;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO sync_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO sync_user;
```

### Performance Monitoring

Monitor these metrics:
- Sync duration per table
- Number of rows processed per batch
- Watermark progression
- Memory and CPU usage

Adjust `BATCH_SIZE` based on:
- Available memory
- Network latency
- Database performance
- Table row size

## Security Considerations

### Database Security

1. **Dedicated Sync User:**
```sql
-- Create dedicated user for sync operations
CREATE USER sync_user WITH PASSWORD 'strong_password';

-- Grant minimal required permissions
GRANT CONNECT ON DATABASE source_db TO sync_user;
GRANT USAGE ON SCHEMA public TO sync_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sync_user;
```

2. **SSL/TLS Configuration:**
```bash
# Always use SSL in production
SOURCE_DSN="postgres://user:pass@host:5432/db?sslmode=require"

# For maximum security, use certificate verification
TARGET_DSN="postgres://user:pass@host:5432/db?sslmode=verify-full&sslcert=client.crt&sslkey=client.key&sslrootcert=ca.crt"
```

### Container Security

1. **Run as non-root user** (already configured in Dockerfile)
2. **Use secrets management:**
```bash
# Use Docker secrets
docker service create \
  --secret source_dsn \
  --secret target_dsn \
  syncsvc:v1.0.0
```

3. **Network isolation:**
```yaml
networks:
  database-network:
    driver: overlay
    encrypted: true
```

### Credentials Management

Never store credentials in:
- Container images
- Configuration files
- Environment variables in compose files

Use instead:
- Kubernetes secrets
- Docker secrets
- External secret management (HashiCorp Vault, AWS Secrets Manager)
- Environment variables loaded at runtime

### Production Checklist

- [ ] SSL/TLS enabled for database connections
- [ ] Dedicated database user with minimal permissions
- [ ] Secrets stored securely (not in code/configs)
- [ ] Network traffic encrypted and isolated
- [ ] Monitoring and alerting configured
- [ ] Regular security updates for base images
- [ ] Backup and disaster recovery plan
- [ ] Resource limits configured
- [ ] Log collection and retention policies
- [ ] Health checks and restart policies configured

## Example Production Setup

```bash
# 1. Build and tag image
docker build -t registry.company.com/syncsvc:v1.0.0 .
docker push registry.company.com/syncsvc:v1.0.0

# 2. Create production config
cat > /etc/syncsvc/config.json << EOF
{
  "tables": [
    {
      "name": "public.security_findings",
      "key_columns": ["asset_id", "vulnerability_id", "scan_id"],
      "sync_column": "discovered_at"
    },
    {
      "name": "public.network_assets",
      "key_columns": ["ip_address", "port"],
      "sync_column": "last_seen"
    }
  ]
}
EOF

# 3. Set secure environment variables
export SOURCE_DSN="postgres://sync_prod:$PROD_PASSWORD@prod-scanner-db.internal:5432/scanner?sslmode=require"
export TARGET_DSN="postgres://sync_prod:$PROD_PASSWORD@analytics-db.internal:5432/analytics?sslmode=require"

# 4. Run with monitoring
docker run --rm \
  --name database-sync \
  -e SOURCE_DSN="$SOURCE_DSN" \
  -e TARGET_DSN="$TARGET_DSN" \
  -e BATCH_SIZE="10000" \
  -v /etc/syncsvc/config.json:/root/config.json:ro \
  --network production \
  --memory="1g" \
  --cpus="2" \
  registry.company.com/syncsvc:v1.0.0
```