# 网络服务 (Netsrv)

## 🚀 **功能特性**

### **核心功能**
- **数据转发**: 从Redis获取数据，自动转发到MQTT和HTTP目标
- **智能分组**: 按数据类型自动分组，支持批量发送和消息分割
- **单点读取**: 响应MQTT单点读取请求，实时查询Redis数据
- **设备管理**: 自动管理设备上线/下线状态
- **配置热更新**: 支持运行时重新加载配置文件

### **数据协议支持**
- **MQTT**: 支持SSL/TLS加密连接，QoS级别可配置
- **HTTP/HTTPS**: 异步HTTP客户端，支持重试和超时
- **Redis**: 支持多种数据类型（string、hash、list、set）
- **JSON**: 标准JSON数据格式，支持嵌套结构

### **监控和管理**
- **健康检查**: RESTful API健康状态监控
- **实时日志**: 结构化日志记录，支持文件轮转
- **性能指标**: 数据转发统计和连接状态监控
- **配置管理**: 动态配置查看和更新

## 项目简介

网络服务是VoltageEMS微服务架构中的核心组件，负责将本地数据推送到外部云服务和第三方系统。该服务支持多种网络协议，包括MQTT、HTTP等，并集成了主流云平台如阿里云IoT等。

## 主要功能

- **数据转发**: 从Redis获取数据，推送到外部云服务
- **多协议支持**: 支持MQTT、HTTP、阿里云IoT等协议
- **可配置转发**: 支持动态配置转发目标和数据格式
- **连接管理**: 自动重连、连接池管理、状态监控
- **REST API**: 提供完整的API接口进行服务管理
- **实时监控**: 实时监控转发状态和连接状态

## 技术架构

- **Python 3.10.12**: 核心开发语言
- **FastAPI**: 现代化Web框架，提供高性能API
- **Redis**: 数据源，从其他微服务获取数据
- **MQTT**: 轻量级消息传输协议
- **HTTP/HTTPS**: 标准HTTP协议支持
- **异步编程**: 基于asyncio的高并发处理

## 项目结构

```
netsrv/
├── app/                    # 应用代码
│   ├── api/               # API路由
│   │   └── routes.py      # 路由定义
│   ├── core/              # 核心模块
│   │   ├── config.py      # 配置管理
│   │   ├── database.py    # Redis连接
│   │   ├── mqtt_client.py # MQTT客户端
│   │   ├── http_client.py # HTTP客户端
│   │   └── logger.py      # 日志配置
│   └── services/          # 业务服务
│       └── data_forwarder.py # 数据转发服务
├── config/                 # 配置文件
│   └── forward_configs.yaml # 转发配置示例
├── logs/                   # 日志文件
├── main.py                 # 主程序入口
├── requirements.txt        # Python依赖
├── Dockerfile             # Docker镜像构建
└── README.md              # 项目说明
```

## 快速开始

### 环境要求

- Python 3.10.12+
- Redis 6.0+
- MQTT Broker (可选)

### 安装依赖

```bash
pip install -r requirements.txt
```

### 配置环境变量

复制 `.envshow` 文件为 `.env` 并修改配置：

```bash
cp .envshow .env
```

主要配置项：
- `REDIS_HOST`: Redis服务器地址
- `MQTT_BROKER_HOST`: MQTT代理地址
- `PORT`: 服务端口 (默认6006)

### 配置文件说明

项目使用两层配置结构：

1. **应用配置** (`.env` 文件)
   - 服务器设置、端口、Redis连接等基础配置
   - 环境相关的配置项

2. **业务配置** (`config/netsrv.yaml`)
   - MQTT主题、HTTP端点、转发规则等业务配置
   - 需要经常修改的配置项
   - Docker部署时挂载到 `/extp/config` 目录

### 启动服务

```bash
python main.py
```

服务将在 `http://localhost:6006` 启动

### API文档

启动服务后，访问以下地址查看API文档：
- Swagger UI: `http://localhost:6005/docs`
- ReDoc: `http://localhost:6005/redoc`

## 核心API接口

### 健康检查
- `GET /api/v1/health` - 服务健康状态

### 服务状态
- `GET /api/v1/status` - 获取服务详细状态

### 数据转发管理
- `POST /api/v1/forwarder/start` - 启动转发服务
- `POST /api/v1/forwarder/stop` - 停止转发服务
- `GET /api/v1/forwarder/configs` - 获取转发配置
- `POST /api/v1/forwarder/configs` - 添加转发配置
- `DELETE /api/v1/forwarder/configs/{name}` - 删除转发配置

### Redis操作
- `GET /api/v1/redis/keys` - 获取Redis键列表
- `GET /api/v1/redis/keys/{key}` - 获取Redis键值

### MQTT操作
- `POST /api/v1/mqtt/publish` - 发布MQTT消息
- `POST /api/v1/mqtt/subscribe` - 订阅MQTT主题
- `GET /api/v1/mqtt/connection` - 获取连接状态
- `POST /api/v1/mqtt/connect` - 手动连接
- `POST /api/v1/mqtt/disconnect` - 断开连接

## 转发配置示例

### MQTT转发
```yaml
mqtt_forward:
  name: "mqtt_forward"
  type: "mqtt"
  topic: "netsrv/data"
  qos: 0
```

### HTTP转发
```yaml
http_forward:
  name: "http_forward"
  type: "http"
  url: "http://api.example.com/data"
  headers:
    Authorization: "Bearer token"
```

### 阿里云IoT转发
```yaml
aliyun_forward:
  name: "aliyun_forward"
  type: "aliyun"
  url: "https://iot.cn-shanghai.aliyuncs.com/"
  format:
    product_sn: "your-product-sn"
    device_sn: "your-device-sn"
```

## Docker部署

### 构建镜像
```bash
docker build -t netsrv:latest .
```

### 运行容器
```bash
docker run -d \
  --name netsrv \
  --restart unless-stopped \
  -p 6006:6006 \
  -v /extp/config:/app/config \
  -v /extp/logs:/app/logs \
  --env-file .env \
  netsrv:latest
```

## 开发说明

### 代码规范
- 使用类型注解
- 遵循PEP 8代码风格
- 添加适当的文档字符串
- 使用loguru进行日志记录

### 测试
```bash
# 运行测试
python -m pytest

# 运行覆盖率测试
python -m pytest --cov=app
```

### 日志
日志文件位于 `logs/` 目录：
- `netsrv.log`: 主日志文件
- `error.log`: 错误日志文件

## 故障排除

### 常见问题

1. **Redis连接失败**
   - 检查Redis服务是否运行
   - 验证连接参数配置

2. **MQTT连接失败**
   - 检查MQTT代理地址和端口
   - 验证认证信息

3. **数据转发失败**
   - 检查转发配置是否正确
   - 查看日志文件了解详细错误

### 日志分析
```bash
# 查看实时日志
tail -f logs/netsrv.log

# 查看错误日志
tail -f logs/error.log
```

## 贡献指南

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 联系方式

- 项目维护者: VoltageEMS Team
- 项目地址: [GitHub Repository]
- 问题反馈: [Issues]

## 更新日志

### v1.0.0 (2024-01-XX)
- 初始版本发布
- 支持MQTT、HTTP数据转发
- 集成阿里云IoT平台
- 提供完整的REST API
- Docker容器化支持
