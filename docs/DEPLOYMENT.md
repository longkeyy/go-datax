# DataX Deployment Guide

## GitHub Release and Packages Setup

### 1. GitHub Release (✅ Completed)

已成功创建 v1.3.0 release，包含8个平台的二进制文件：

- **Linux**: amd64, arm64
- **macOS**: amd64 (Intel), arm64 (Apple Silicon)
- **Windows**: amd64, arm64
- **FreeBSD**: amd64, arm64

**Release URL**: https://github.com/longkeyy/go-datax/releases/tag/v1.3.0

### 2. Docker Support (✅ Completed)

已添加Docker支持，包括：
- 优化的多阶段Dockerfile（最终镜像 <15MB）
- .dockerignore文件优化构建上下文
- 基于scratch的最小化镜像

#### 本地构建Docker镜像

```bash
# 构建镜像
docker build -t ghcr.io/longkeyy/go-datax:v1.3.0 .

# 运行容器
docker run --rm ghcr.io/longkeyy/go-datax:v1.3.0 --help
```

### 3. GitHub Container Registry Setup

要完成GitHub Container Registry的设置，需要具有`workflow`权限的GitHub token：

#### 手动推送镜像

```bash
# 1. 创建具有以下权限的GitHub Personal Access Token:
#    - repo (完整仓库访问)
#    - write:packages (写入包权限)
#    - workflow (工作流权限)

# 2. 登录GitHub Container Registry
echo YOUR_TOKEN | docker login ghcr.io -u longkeyy --password-stdin

# 3. 构建并推送镜像
docker build -t ghcr.io/longkeyy/go-datax:v1.3.0 -t ghcr.io/longkeyy/go-datax:latest .
docker push ghcr.io/longkeyy/go-datax:v1.3.0
docker push ghcr.io/longkeyy/go-datax:latest
```

### 4. GitHub Actions自动化 (需要workflow权限)

已准备好GitHub Actions工作流文件(`.github/workflows/release.yml`)，包含：

- 自动跨平台构建
- GitHub Release创建
- Docker镜像构建和推送
- 多架构支持

要启用自动化，需要：

1. 确保GitHub CLI有`workflow`权限
2. 添加workflow文件到仓库：

```bash
git add .github/workflows/release.yml
git commit -m "feat: add GitHub Actions workflow for automated releases"
git push origin main
```

### 5. 使用部署的包

#### 下载二进制文件

```bash
# Linux x86_64
wget https://github.com/longkeyy/go-datax/releases/download/v1.3.0/datax-linux-amd64
chmod +x datax-linux-amd64
sudo mv datax-linux-amd64 /usr/local/bin/datax

# macOS Apple Silicon
wget https://github.com/longkeyy/go-datax/releases/download/v1.3.0/datax-darwin-arm64
chmod +x datax-darwin-arm64
sudo mv datax-darwin-arm64 /usr/local/bin/datax
```

#### 使用Docker镜像

```bash
# 一旦推送到GitHub Container Registry
docker pull ghcr.io/longkeyy/go-datax:latest

# 运行数据同步
docker run --rm -v $(pwd)/config.json:/config.json ghcr.io/longkeyy/go-datax:latest -job /config.json
```

### 6. Go Module包

Go模块包已自动通过GitHub release可用：

```bash
go get github.com/longkeyy/go-datax@v1.3.0
```

## 总结

✅ **已完成**:
- GitHub Release v1.3.0（8个平台二进制文件）
- Docker支持和优化的Dockerfile
- 跨平台构建支持
- 部署文档

⏳ **待完成**（需要workflow权限）:
- GitHub Container Registry镜像推送
- GitHub Actions自动化工作流

所有核心功能已就绪，只需要具有适当权限的GitHub token即可完成容器注册表的设置。