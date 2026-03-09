# 🐳 Docker 部署快速指南

本文档是Docker部署的快速参考指南。完整文档请查看 [DOCKER_DEPLOYMENT.md](DOCKER_DEPLOYMENT.md)。

## 🎯 为什么选择Docker部署？

- ✅ **零环境配置**：无需安装Python、Node.js等环境
- ✅ **一键启动**：简单命令即可运行
- ✅ **环境隔离**：不影响系统其他软件
- ✅ **跨平台**：Windows/macOS/Linux统一部署方式
- ✅ **稳定可靠**：容器自动重启，故障自动恢复

## ⚡ 5分钟快速开始

### 1️⃣ 安装Docker

**Windows/macOS:**
- 下载 [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- 双击安装，启动Docker Desktop

**Linux:**
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```

### 2️⃣ 配置环境变量

```bash
# 复制配置模板
cp .env.template .env

# 编辑 .env 文件，填入你的 DeepSeek API Key
# DEEPSEEK_API_KEY=sk-your-actual-api-key-here
```

### 3️⃣ 启动服务

```bash
# 一键启动
docker-compose up -d

# 查看日志（可选）
docker-compose logs -f
```

### 4️⃣ 访问系统

打开浏览器访问：**http://localhost:8503**

## 🎮 常用命令

```bash
# 启动服务
docker-compose up -d

# 停止服务
docker-compose down

# 重启服务
docker-compose restart

# 查看日志
docker-compose logs -f

# 查看运行状态
docker ps

# 进入容器（调试用）
docker exec -it agentsstock1 bash
```

## 📁 项目结构

```
agentsstock1/
├── Dockerfile              # Docker镜像构建文件
├── docker-compose.yml      # Docker编排配置
├── .dockerignore          # Docker构建忽略文件
├── .env.template          # 环境变量模板
├── .env                   # 环境变量配置（需自己创建）
├── DOCKER_DEPLOYMENT.md   # 详细部署文档
├── DOCKER_CHECKLIST.md    # 部署检查清单
└── README.md              # 项目主文档
```

## 🔑 关键特性

### Node.js 环境集成
Docker镜像已内置Node.js 18.x环境，支持pywencai等需要Node.js的Python包。

### 数据持久化
以下数据会自动保存到宿主机：
- `investment.db` - 统一投资域数据库
- `data/` - 临时数据目录

即使删除容器，数据也不会丢失。

### 自动迁移说明

当前版本启动后会自动把旧库迁移到 `investment.db`，但前提是运行环境里能看到这些旧文件：

- `stock_analysis.db`
- `portfolio_stocks.db`
- `smart_monitor.db`
- `monitoring.db`

如果你是 Docker 升级部署，首次启动容器时要把这些旧库也挂进去；迁移确认完成后，再移除旧库挂载即可。

### 健康检查
容器会自动监控应用健康状态，异常时自动重启。

## 🐛 快速故障排除

### 问题1: 容器启动失败
```bash
# 查看详细日志
docker-compose logs

# 检查 .env 文件是否存在并配置正确
cat .env
```

### 问题2: 无法访问网页
```bash
# 检查容器是否运行
docker ps

# 检查端口映射
docker port agentsstock1

# 尝试从容器内访问
docker exec agentsstock1 curl http://localhost:8501
```

### 问题3: 端口被占用
修改 `docker-compose.yml` 文件：
```yaml
ports:
  - "8502:8501"  # 改用8502端口
```

### 问题4: 数据库权限错误（Linux/macOS）
```bash
chmod 666 *.db
chmod 777 data/
```

## 🔄 更新和维护

### 更新代码
```bash
# 停止服务
docker-compose down

# 拉取最新代码
git pull

# 重新构建并启动
docker-compose up -d --build
```

### 清理和重置
```bash
# 完全清理（包括volumes）
docker-compose down -v

# 删除镜像
docker rmi agentsstock1:latest

# 重新构建
docker-compose build --no-cache
docker-compose up -d
```

### 备份数据
```bash
# 备份统一数据库
cp investment.db investment.db.backup
```

## 📊 性能监控

```bash
# 查看资源使用
docker stats agentsstock1

# 查看容器详情
docker inspect agentsstock1

# 查看日志大小
docker logs agentsstock1 2>&1 | wc -l
```

## 🔒 安全建议

1. **保护.env文件**
   ```bash
   # Linux/macOS - 设置文件权限
   chmod 600 .env
   ```

2. **不要提交敏感信息**
   - `.env` 文件已在 `.gitignore` 中
   - 不要将API Key提交到版本控制

3. **定期更新**
   ```bash
   # 更新Docker镜像
   docker-compose pull
   docker-compose up -d
   ```

## 🌐 生产环境部署

对于生产环境，建议：

1. 使用反向代理（Nginx/Caddy）
2. 配置HTTPS证书
3. 设置防火墙规则
4. 配置日志轮转
5. 定期备份数据库
6. 监控容器状态

示例Nginx配置：
```nginx
server {
    listen 80;
    server_name yourdomain.com;

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

## 📚 更多资源

- **完整部署文档**: [DOCKER_DEPLOYMENT.md](DOCKER_DEPLOYMENT.md)
- **部署检查清单**: [DOCKER_CHECKLIST.md](DOCKER_CHECKLIST.md)
- **项目主文档**: [README.md](README.md)
- **Docker官方文档**: https://docs.docker.com/

## 🆘 获取帮助

遇到问题？

1. 查看日志：`docker-compose logs -f`
2. 查看完整文档：[DOCKER_DEPLOYMENT.md](DOCKER_DEPLOYMENT.md)
3. 检查部署清单：[DOCKER_CHECKLIST.md](DOCKER_CHECKLIST.md)
4. 联系支持：ws3101001@126.com

---

**Docker让部署更简单！祝您使用愉快！** 🚀🐳

