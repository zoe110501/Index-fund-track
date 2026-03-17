# OpenAI Codex CLI - D:\codex

## 使用方法

### 方式1：直接运行
```
D:\codex\node_modules\.bin\codex.cmd
```

### 方式2：使用批处理脚本
```
D:\codex\codex.bat
```

### 方式3：通过 npx（任意目录）
```
npx @openai/codex
```

## 配置 API Key

在运行前需要设置 OpenAI API Key：

```
set OPENAI_API_KEY=你的API密钥
```

或者在系统环境变量中永久设置 `OPENAI_API_KEY`。

## 基本用法

```
# 询问代码问题
codex "写一个 Python 排序算法"

# 在当前目录执行任务
codex "创建一个 Flask 应用"
```

## 版本信息
- codex-cli: 0.104.0
- Node.js: v24.13.1
