# Migration from cx_Oracle to oracledb

## 迁移指南 (Migration Guide)

本项目已从 `cx_Oracle` 迁移到 `oracledb`，这是Oracle官方的新Python驱动程序。

This project has migrated from `cx_Oracle` to `oracledb`, Oracle's new official Python driver.

---

## 为什么迁移？ (Why Migrate?)

### 问题 (Problem)
使用 `cx_Oracle` 时，安装过程中会出现以下错误：
```
Building wheel for cx_Oracle (pyproject.toml) ... error
exit code: 1
```

原因：`cx_Oracle` 需要编译C扩展，并依赖Oracle Instant Client库。

When using `cx_Oracle`, installation fails with compilation errors because it requires Oracle Instant Client to be installed on the system.

### 解决方案 (Solution)
使用 `oracledb` 替代 `cx_Oracle`。`oracledb` 是Oracle官方的下一代Python驱动：

- ✅ **无需编译**: 提供预编译的wheel包
- ✅ **无需Oracle Client**: Thin模式纯Python实现
- ✅ **兼容API**: 与cx_Oracle API完全兼容
- ✅ **官方支持**: Oracle官方维护
- ✅ **跨平台**: 支持所有主流操作系统

---

## 迁移步骤 (Migration Steps)

### 1. 更新依赖 (Update Dependencies)

**旧版 (Old)**:
```txt
cx_Oracle>=8.3.0
```

**新版 (New)**:
```txt
oracledb>=1.4.0
```

### 2. 更新导入 (Update Imports)

**旧版 (Old)**:
```python
import cx_Oracle

dsn = cx_Oracle.makedsn(host, port, service_name=service)
conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
```

**新版 (New)**:
```python
import oracledb

dsn = oracledb.makedsn(host, port, service_name=service)
conn = oracledb.connect(user=user, password=password, dsn=dsn)
```

### 3. API兼容性 (API Compatibility)

`oracledb` 提供与 `cx_Oracle` 完全兼容的API：

| cx_Oracle | oracledb | 说明 |
|-----------|----------|------|
| `cx_Oracle.connect()` | `oracledb.connect()` | ✅ 兼容 |
| `cx_Oracle.makedsn()` | `oracledb.makedsn()` | ✅ 兼容 |
| `cx_Oracle.LOB` | `oracledb.LOB` | ✅ 兼容 |
| `cx_Oracle.Cursor` | `oracledb.Cursor` | ✅ 兼容 |

---

## 两种模式 (Two Modes)

### Thin 模式 (默认 / Default)

纯Python实现，无需Oracle Instant Client：

```python
import oracledb

# Thin模式自动启用 (Thin mode enabled by default)
conn = oracledb.connect(user="user", password="pass", dsn="host:1521/service")
```

**优点**:
- 无需安装Oracle Client
- 跨平台兼容性好
- 安装简单快速

**适用场景**:
- 大多数应用场景
- 云环境部署
- Docker容器化

### Thick 模式 (可选 / Optional)

使用Oracle Instant Client，提供完整功能：

```python
import oracledb

# 启用Thick模式 (Enable thick mode)
oracledb.init_oracle_client(lib_dir="/path/to/instantclient")

conn = oracledb.connect(user="user", password="pass", dsn="host:1521/service")
```

**优点**:
- 支持所有Oracle高级特性
- 性能优化
- 完全兼容cx_Oracle

**适用场景**:
- 需要高级Oracle特性
- 性能关键应用
- 从cx_Oracle迁移需要完全兼容

---

## 安装 (Installation)

### 快速安装 (Quick Install)

```bash
pip install oracledb
```

### 从 requirements.txt 安装

```bash
pip install -r requirements.txt
```

### 验证安装 (Verify Installation)

```python
import oracledb
print(f"oracledb version: {oracledb.version}")
print(f"Thin mode: {oracledb.is_thin_mode()}")
```

---

## 常见问题 (FAQ)

### Q1: 我需要卸载cx_Oracle吗？

是的，建议卸载以避免冲突：
```bash
pip uninstall cx_Oracle
pip install oracledb
```

### Q2: 现有代码需要改动吗？

大多数情况下只需要更改import语句：
```python
# import cx_Oracle  # 旧
import oracledb     # 新
```

### Q3: Thin模式有限制吗？

Thin模式支持大多数常用功能。如需高级特性（如高级队列AQ），可使用Thick模式。

### Q4: 性能会受影响吗？

对于大多数应用，Thin模式性能足够。如需最佳性能，可使用Thick模式。

### Q5: 如何选择模式？

- **Thin模式**: 推荐用于新项目和大多数场景
- **Thick模式**: 需要cx_Oracle完全兼容或高级特性时使用

---

## 参考资料 (References)

- [python-oracledb 官方文档](https://python-oracledb.readthedocs.io/)
- [迁移指南](https://python-oracledb.readthedocs.io/en/latest/user_guide/appendix_a.html)
- [Thin vs Thick 模式对比](https://python-oracledb.readthedocs.io/en/latest/user_guide/initialization.html)
- [GitHub Repository](https://github.com/oracle/python-oracledb)

---

## 技术支持 (Support)

如遇到问题，请：
1. 查看 [官方文档](https://python-oracledb.readthedocs.io/)
2. 搜索 [GitHub Issues](https://github.com/oracle/python-oracledb/issues)
3. 参考本项目的 `USER_GUIDE.md` 故障排查部分
