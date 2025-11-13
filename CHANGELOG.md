# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### BREAKING CHANGES
- **PostgreSQL and MySQL Writer Behavior Change**: Changed conflict handling from SKIP to UPSERT strategy
  - **Previous Behavior**: Used `ON CONFLICT DO NOTHING` (PostgreSQL) and `INSERT IGNORE` (MySQL) to skip conflicting records
  - **New Behavior**: Automatically queries primary keys and updates non-PK columns on conflicts using `ON CONFLICT DO UPDATE` (PostgreSQL) and `ON DUPLICATE KEY UPDATE` (MySQL)
  - **Impact**: Existing records will now be updated instead of skipped when primary key conflicts occur
  - **Edge Cases Handled**:
    - Tables with no primary key: Falls back to standard INSERT
    - All-primary-key tables (e.g., junction tables): Falls back to DO NOTHING/IGNORE to avoid syntax errors
    - Composite primary keys: Fully supported with proper conflict detection
  - **Migration Note**: If your workload relies on skipping duplicates, you may see different behavior. Consider reviewing your data sync logic.

### Changed
- **架构改进**: 重构为无状态设计，支持灵活的执行模式
  - 新增 `core/job/job.go`: 实现了符合 Go 哲学的 `Job` 结构体，类似 `exec.Cmd` 模式
  - `Job` 提供 `Start()` (异步), `Wait()` (阻塞), `Run()` (同步) 方法
  - `Job` 提供 `Status()` 方法实时查询任务状态和进度
  - `Job` 提供 `Done()` channel 用于 Go 风格的完成信号
  - 任务完成后自动释放重对象 (`JobContainer`)，只保留轻量级统计摘要
  - `JobContainer` 新增 `StartWithContext()` 方法支持 context 取消机制
  - `JobContainer.Start()` 保持向后兼容，内部调用 `StartWithContext()`
  - `Engine` 简化为无状态设计，删除未使用的 `configuration` 字段
  - CLI 模式完全向后兼容，无破坏性变更

### Added
- **Multi-Table Write Support with Intuitive Channel Semantics**: Enhanced multi-table split logic beyond DataX Java
  - **Channel Semantic**: `channel` parameter always means "concurrency per table" (not total tasks)
  - **Single-Table Mode**: `channel: N` → Creates N parallel tasks for that table
  - **Multi-Table Mode**: `channel: N` → Creates N tasks per table (total = tableCount × N)
  - **Go Enhancement**: More intuitive than Java's restrictive "tableCount must equal taskCount" constraint
  - **SQL Placeholder Support**: `@table` placeholder in preSql/postSql is replaced with actual table name per task
  - **Example Configurations**:
    ```json
    // Single table with 6 concurrent tasks
    {
      "connection": [{"table": ["users"]}],
      "channel": 6
    }
    → Creates 6 tasks, all writing to users table

    // Multi-table: each table gets 6 concurrent tasks
    {
      "connection": [{"table": ["users", "orders", "products"]}],
      "channel": 6
    }
    → Creates 18 tasks total (3 tables × 6 tasks per table)
    → users: 6 concurrent tasks with independent PK detection
    → orders: 6 concurrent tasks with independent PK detection
    → products: 6 concurrent tasks with independent PK detection
    ```

- **Primary Key-Based Upsert for PostgreSQL and MySQL**:
  - Automatically queries and caches primary key columns during task initialization
  - Supports single and composite primary keys
  - Generates database-specific upsert SQL:
    - PostgreSQL: `ON CONFLICT (pk) DO UPDATE SET non_pk_cols=EXCLUDED.non_pk_cols`
    - MySQL: `ON DUPLICATE KEY UPDATE non_pk_cols=VALUES(non_pk_cols)`
  - Edge case handling:
    - No primary key: Standard INSERT
    - All-primary-key tables: DO NOTHING/IGNORE
    - Query failures: Graceful fallback to standard INSERT

- **Job 状态管理**:
  - `JobState`: CREATED, RUNNING, SUCCEEDED, FAILED, CANCELLED
  - `JobStatus`: 包含 ID, State, StartTime, EndTime, Progress, Stats, Error
  - `ProgressInfo`: 实时进度信息（记录数、字节数、速度、百分比）
  - `StatsSummary`: 轻量级最终统计摘要（完成后保留）

- **Context 取消支持**:
  - 在 `schedule()` 循环中检查 context 取消信号
  - 优雅关闭：等待已启动的 task group 完成

### Benefits
- **内核灵活性**: 同一内核既可用于 CLI 阻塞模式，也可用于未来的 HTTP Service 异步模式
- **内存管理**: 任务完成后自动释放重对象，避免内存泄漏
- **Go 最佳实践**: 遵循标准库模式（`exec.Cmd`, `context.Context`, `Done()` channel）
- **可观测性**: 通过 `Job.Status()` 实时监控任务进度和指标
- **可扩展性**: 为未来的 Service 模式、gRPC 模式、消息队列模式打下基础

## [v1.4.0] - 2025-09-30

### Added
- Initial release with core functionality
- Support for multiple data sources (MySQL, PostgreSQL, Oracle, MongoDB, etc.)
- Plugin-based architecture
- Real-time statistics and progress reporting
