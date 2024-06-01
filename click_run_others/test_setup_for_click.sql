-- 创建字符类型表 CHAR_TBL
CREATE TABLE CHAR_TBL (
    f1 String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 CHAR_TBL
INSERT INTO CHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd');

-- 创建浮点数类型表 FLOAT8_TBL
CREATE TABLE FLOAT8_TBL (
    f1 Float64
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 FLOAT8_TBL
INSERT INTO FLOAT8_TBL (f1) VALUES
  (0.0),
  (-34.84),
  (-1004.30),
  (-1.2345678901234e+200),
  (-1.2345678901234e-200);

-- 创建整数类型表 INT2_TBL
CREATE TABLE INT2_TBL (
    f1 Int16
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 INT2_TBL
INSERT INTO INT2_TBL (f1) VALUES
  (0),
  (1234),
  (-1234),
  (32767),
  (-32767);

-- 创建整数类型表 INT4_TBL
CREATE TABLE INT4_TBL (
    f1 Int32
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 INT4_TBL
INSERT INTO INT4_TBL (f1) VALUES
  (0),
  (123456),
  (-123456),
  (2147483647),
  (-2147483647);

-- 创建大整数类型表 INT8_TBL
CREATE TABLE INT8_TBL (
    q1 Int64,
    q2 Int64
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 INT8_TBL
INSERT INTO INT8_TBL VALUES
  (123, 456),
  (123, 4567890123456789),
  (4567890123456789, 123),
  (4567890123456789, 4567890123456789),
  (4567890123456789, -4567890123456789);

-- 创建点类型表 POINT_TBL
CREATE TABLE POINT_TBL (
    f1 String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 POINT_TBL
INSERT INTO POINT_TBL (f1) VALUES
  ('(0.0,0.0)'),
  ('(-10.0,0.0)'),
  ('(-3.0,4.0)'),
  ('(5.1, 34.5)'),
  ('(-5.0,-12.0)'),
  ('(1e-300,-1e-300)'),
  ('(1e+300,Inf)'),
  ('(Inf,1e+300)'),
  ('(NaN, NaN)'),
  ('10.0,10.0');

-- 创建文本类型表 TEXT_TBL
CREATE TABLE TEXT_TBL (
    f1 String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 TEXT_TBL
INSERT INTO TEXT_TBL VALUES
  ('doh!'),
  ('hi de ho neighbor');

-- 创建变长字符类型表 VARCHAR_TBL
CREATE TABLE VARCHAR_TBL (
    f1 String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 插入数据到 VARCHAR_TBL
INSERT INTO VARCHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd');

-- 创建一般数据表 onek, onek2, tenk1, tenk2
CREATE TABLE onek (
    unique1 Int32,
    unique2 Int32,
    two Int32,
    four Int32,
    ten Int32,
    twenty Int32,
    hundred Int32,
    thousand Int32,
    twothousand Int32,
    fivethous Int32,
    tenthous Int32,
    odd Int32,
    even Int32,
    stringu1 String,
    stringu2 String,
    string4 String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 注意：在 ClickHouse 中使用 INSERT INTO ... SELECT 或通过 clickhouse-client 导入数据
-- 创建 onek2 表，从 onek 表中复制结构和数据，并指定存储引擎
CREATE TABLE onek2 ENGINE = MergeTree() ORDER BY tuple() AS
SELECT *
FROM onek;

-- 创建 tenk1 表，从 onek 表中复制结构和数据，并指定存储引擎
CREATE TABLE tenk1 ENGINE = MergeTree() ORDER BY tuple() AS
SELECT *
FROM onek;

-- 创建 tenk2 表，从 tenk1 表中复制结构和数据，并指定存储引擎
CREATE TABLE tenk2 ENGINE = MergeTree() ORDER BY tuple() AS
SELECT *
FROM tenk1;


-- 创建继承表 person, emp, student, stud_emp
-- 注意：ClickHouse 不支持表继承，每个表需要独立创建
CREATE TABLE person (
    name String,
    age Int32,
    location String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 创建 emp 表
CREATE TABLE emp (
    name String,
    age Int32,
    location String,
    salary Int32,
    manager String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 创建 student 表
CREATE TABLE student (
    name String,
    age Int32,
    location String,
    gpa Float64
) ENGINE = MergeTree()
ORDER BY tuple();

-- 创建 stud_emp 表
CREATE TABLE stud_emp (
    name String,
    age Int32,
    location String,
    salary Int32,
    manager String,
    percent Int32
) ENGINE = MergeTree()
ORDER BY tuple();

-- 创建路名表 road, ihighway, shighway
CREATE TABLE road (
    name String,
    thepath String
) ENGINE = MergeTree()
ORDER BY tuple();

-- 创建 ihighway 表，从 road 表中筛选出符合条件的记录，并指定存储引擎
CREATE TABLE ihighway
ENGINE = MergeTree()
ORDER BY tuple() AS
SELECT *
FROM road
WHERE match(name, 'I- .*');

-- 创建 shighway 表，从 road 表中筛选出符合条件的记录，增加一个默认值为 'asphalt' 的新列 surface，并指定存储引擎
CREATE TABLE shighway
ENGINE = MergeTree()
ORDER BY tuple() AS
SELECT *, 'asphalt' AS surface
FROM road
WHERE match(name, 'State Hwy.*');

