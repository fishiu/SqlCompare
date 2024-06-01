-- 创建 CHAR_TBL
CREATE TABLE CHAR_TBL (
    f1 CHAR(4)
);

-- 插入数据到 CHAR_TBL
INSERT INTO CHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd');

-- 创建 FLOAT8_TBL
CREATE TABLE FLOAT8_TBL (
    f1 DOUBLE
);

-- 插入数据到 FLOAT8_TBL
INSERT INTO FLOAT8_TBL (f1) VALUES
  (0.0),
  (-34.84),
  (-1004.30),
  (-1.2345678901234e+200),
  (-1.2345678901234e-200);

-- 创建 INT2_TBL
CREATE TABLE INT2_TBL (
    f1 SMALLINT
);

-- 插入数据到 INT2_TBL
INSERT INTO INT2_TBL (f1) VALUES
  (0),
  (1234),
  (-1234),
  (32767),
  (-32767);

-- 创建 INT4_TBL
CREATE TABLE INT4_TBL (
    f1 INT
);

-- 插入数据到 INT4_TBL
INSERT INTO INT4_TBL (f1) VALUES
  (0),
  (123456),
  (-123456),
  (2147483647),
  (-2147483647);

-- 创建 INT8_TBL
CREATE TABLE INT8_TBL (
    q1 BIGINT,
    q2 BIGINT
);

-- 插入数据到 INT8_TBL
INSERT INTO INT8_TBL VALUES
  (123, 456),
  (123, 4567890123456789),
  (4567890123456789, 123),
  (4567890123456789, 4567890123456789),
  (4567890123456789, -4567890123456789);

-- 创建 POINT_TBL, MySQL 用 TEXT 存储点
CREATE TABLE POINT_TBL (
    f1 TEXT
);

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

-- 创建 TEXT_TBL
CREATE TABLE TEXT_TBL (
    f1 TEXT
);

-- 插入数据到 TEXT_TBL
INSERT INTO TEXT_TBL (f1) VALUES
  ('doh!'),
  ('hi de ho neighbor');

-- 创建 VARCHAR_TBL
CREATE TABLE VARCHAR_TBL (
    f1 VARCHAR(4)
);

-- 插入数据到 VARCHAR_TBL
INSERT INTO VARCHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd');

-- 创建 onek 和 onek2 表
CREATE TABLE onek (
    unique1 INT,
    unique2 INT,
    two INT,
    four INT,
    ten INT,
    twenty INT,
    hundred INT,
    thousand INT,
    twothousand INT,
    fivethous INT,
    tenthous INT,
    odd INT,
    even INT,
    stringu1 VARCHAR(255),
    stringu2 VARCHAR(255),
    string4 VARCHAR(255)
);

-- MySQL 中使用 LOAD DATA INFILE 代替 COPY
-- LOAD DATA INFILE '/data/onek.data' INTO TABLE onek;
-- 注意：LOAD DATA INFILE 需要文件在服务器上，并且要有相应的文件权限

CREATE TABLE onek2 AS SELECT * FROM onek;

-- 创建 tenk1 和 tenk2 表
CREATE TABLE tenk1 LIKE onek;
-- LOAD DATA INFILE '/data/tenk.data' INTO TABLE tenk1;

CREATE TABLE tenk2 AS SELECT * FROM tenk1;

-- 创建 person, emp, student, stud_emp 表
CREATE TABLE person (
    name TEXT,
    age INT,
    location TEXT
);

-- LOAD DATA INFILE '/data/person.data' INTO TABLE person;

CREATE TABLE emp (
    salary INT,
    manager VARCHAR(255),
    name TEXT,
    age INT,
    location TEXT
);

-- LOAD DATA INFILE '/data/emp.data' INTO TABLE emp;

CREATE TABLE student (
    gpa DOUBLE,
    name TEXT,
    age INT,
    location TEXT
);

-- LOAD DATA INFILE '/data/student.data' INTO TABLE student;

CREATE TABLE stud_emp (
    percent INT,
    salary INT,
    manager VARCHAR(255),
    name TEXT,
    age INT,
    location TEXT,
    gpa DOUBLE
);

-- LOAD DATA INFILE '/data/stud_emp.data' INTO TABLE stud_emp;

-- 创建 road, ihighway, shighway 表
CREATE TABLE road (
    name TEXT,
    thepath TEXT
);

-- LOAD DATA INFILE '/data/streets.data' INTO TABLE road;

CREATE TABLE ihighway AS
SELECT *
FROM road
WHERE name LIKE 'I-%';

CREATE TABLE shighway AS
SELECT *, 'asphalt' AS surface
FROM road
WHERE name LIKE 'State Hwy%';
