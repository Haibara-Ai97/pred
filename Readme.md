### 流域水文预测系统 - 分布式计算与模型更新框架 

#### 项目简介 

本项目是一个高性能、可扩展的水文预测系统，旨在解决传统Python数据处理中的性能瓶颈和复杂性问题。通过Go语言的并发特性和Python的数据处理能力，构建了一个高效的分布式计算框架。

#### 主要特性

##### 技术架构

- 使用Go语言的goroutine实现并行计算管理
- Python负责核心数据计算和错误处理
- 消息队列实现Go与Python间的通信
- 支持多种数据源读取方式

##### 计算模式

1. 粗差值计算及数据预处理 
2. 预处理数据预测与评判
3. 全流程一体化计算

##### 数据源支持

- 直接数据库读取
- 接口数据读取（待接口完善）

##### 模型更新机制

- Python计算异常触发更新
- 定时更新

#### 项目进展

##### 已完成

- 三种计算模式设计与测试
- 数据源读取接口设计
- 后端触发计算接口
- 基本模型参数定时更新

##### 待完善

- 数据源接口完善
- 模型更新逻辑优化

##### 技术栈

- Go、Python、Gorm、RabbitMQ、Goroutine、Python工作池

#### 快速开始

##### 环境准备

Go 1.22.10

Python 3.6

配置Config和EmergencyConfig

##### 安装步骤

```bash
git clone https://github.com/Haibara-Ai97/pred.git

# 安装依赖
go mod tidy
pip install -r requirement.txt
```

##### 使用示例

```bash
python RabbitMQ.py
go run main.go
```

- **注意**：项目仍在持续开发中，欢迎提出宝贵意见和建议！
