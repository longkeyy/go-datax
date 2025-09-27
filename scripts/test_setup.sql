-- 创建测试数据库
CREATE DATABASE IF NOT EXISTS llm_datasets;
CREATE DATABASE IF NOT EXISTS datax;

-- 切换到llm_datasets数据库
\c llm_datasets;

-- 创建源表并插入测试数据
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO test_table (name, age, email) VALUES
('Alice', 25, 'alice@example.com'),
('Bob', 30, 'bob@example.com'),
('Charlie', 35, 'charlie@example.com'),
('Diana', 28, 'diana@example.com'),
('Eve', 32, 'eve@example.com');

-- 切换到datax数据库
\c datax;

-- 创建目标表
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);