#!/bin/bash

# 批量替换日志系统的脚本
# 将标准库log替换为项目的zap日志系统

set -e

# 获取项目根目录
PROJECT_ROOT="/Users/longkeyy/GolandProjects/go-datax"

# 需要处理的文件列表
FILES=(
    "$PROJECT_ROOT/plugins/reader/clickhousereader/clickhouse_reader.go"
    "$PROJECT_ROOT/plugins/reader/txtfilereader/txtfile_reader.go"
    "$PROJECT_ROOT/plugins/reader/streamreader/stream_reader.go"
    "$PROJECT_ROOT/plugins/reader/jsonfilereader/jsonfile_reader.go"
    "$PROJECT_ROOT/plugins/reader/mongoreader/mongo_reader.go"
    "$PROJECT_ROOT/plugins/reader/dorisreader/doris_reader.go"
    "$PROJECT_ROOT/plugins/reader/sqlserverreader/sqlserver_reader.go"
    "$PROJECT_ROOT/plugins/writer/oraclewriter/oracle_writer.go"
    "$PROJECT_ROOT/plugins/writer/doriswriter/doris_writer.go"
    "$PROJECT_ROOT/plugins/writer/mysqlwriter/mysql_writer.go"
    "$PROJECT_ROOT/plugins/writer/sqlserverwriter/sqlserver_writer.go"
    "$PROJECT_ROOT/plugins/writer/clickhousewriter/clickhouse_writer.go"
    "$PROJECT_ROOT/plugins/writer/txtfilewriter/txtfile_writer.go"
    "$PROJECT_ROOT/plugins/writer/sqlitewriter/sqlite_writer.go"
    "$PROJECT_ROOT/plugins/writer/streamwriter/stream_writer.go"
    "$PROJECT_ROOT/plugins/writer/jsonfilewriter/jsonfile_writer.go"
    "$PROJECT_ROOT/plugins/writer/mongowriter/mongo_writer.go"
)

# 对每个文件进行处理
for file in "${FILES[@]}"; do
    if [[ -f "$file" ]]; then
        echo "Processing $file..."

        # 检查是否包含log.调用
        if grep -q "log\." "$file"; then
            echo "  Found log. calls in $file"

            # 提取组件名 (从文件路径推断)
            component_name=$(basename "$(dirname "$file")" | sed 's/reader$/Reader/g' | sed 's/writer$/Writer/g')
            component_name=$(echo "$component_name" | sed 's/\(.*\)/\U\1/' | sed 's/READER/Reader/g' | sed 's/WRITER/Writer/g')

            # 根据具体的插件类型设置组件名
            case "$component_name" in
                "clickhousereader") component_name="ClickHouseReader" ;;
                "txtfilereader") component_name="TxtFileReader" ;;
                "streamreader") component_name="StreamReader" ;;
                "jsonfilereader") component_name="JsonFileReader" ;;
                "mongoreader") component_name="MongoReader" ;;
                "dorisreader") component_name="DorisReader" ;;
                "sqlserverreader") component_name="SQLServerReader" ;;
                "oraclewriter") component_name="OracleWriter" ;;
                "doriswriter") component_name="DorisWriter" ;;
                "mysqlwriter") component_name="MySQLWriter" ;;
                "sqlserverwriter") component_name="SQLServerWriter" ;;
                "clickhousewriter") component_name="ClickHouseWriter" ;;
                "txtfilewriter") component_name="TxtFileWriter" ;;
                "sqlitewriter") component_name="SQLiteWriter" ;;
                "streamwriter") component_name="StreamWriter" ;;
                "jsonfilewriter") component_name="JsonFileWriter" ;;
                "mongowriter") component_name="MongoWriter" ;;
            esac

            echo "  Using component name: $component_name"

            # 备份原文件
            cp "$file" "${file}.bak"

            # 需要处理的替换（使用临时文件避免并发问题）
            tmp_file=$(mktemp)

            # 首先处理import语句
            if grep -q "\"log\"" "$file"; then
                echo "  Updating imports..."
                # 移除"log"导入，添加logger和zap导入
                sed 's/import (/import (\
	"database\/sql"\
	"fmt"\
	"strings"\
	"time"\
\
	"github.com\/longkeyy\/go-datax\/api\/config"\
	"github.com\/longkeyy\/go-datax\/api\/element"\
	"github.com\/longkeyy\/go-datax\/api\/plugin"\
	"github.com\/longkeyy\/go-datax\/common\/logger"\
	"github.com\/longkeyy\/go-datax\/core\/factory"\
	"go.uber.org\/zap"/' "$file" > "$tmp_file"
                mv "$tmp_file" "$file"

                # 移除单独的"log"导入行
                sed '/"log"/d' "$file" > "$tmp_file"
                mv "$tmp_file" "$file"
            fi

            echo "  File processed successfully"
        else
            echo "  No log. calls found in $file"
        fi
    else
        echo "  File not found: $file"
    fi
done

echo "All files processed!"