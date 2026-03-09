# VLH Auto Entry Robot System

产品需求与技术设计文档 v1.0

## 项目概述

**项目名称**\
VLH Auto Entry Engine

**系统定位**

VLH：链上监听与数据聚合系统\
Robot Engine：策略判断系统\
Execution Service：自动交易执行系统

本系统基于现有
Virtuals‑Launch‑Hunter（VLH）监听系统进行扩展，通过量化指标与策略模型实现自动或半自动打新交易。

核心目标：

-   实时监听链上 Launch 交易数据
-   自动识别新币流动性池
-   量化交易行为特征
-   生成入场评分
-   触发自动打新策略
-   执行自动交易
-   记录交易复盘数据

------------------------------------------------------------------------

# 一、产品背景

Virtuals 打新市场具有以下特点：

-   新币上线节奏快
-   交易行为复杂
-   人工判断延迟较大
-   情绪化交易风险高

现有 VLH 已实现：

-   链上交易监听
-   交易数据聚合
-   实时 UI 面板

但仍缺少：

-   自动识别流动性池
-   交易行为量化
-   自动入场判断
-   自动交易执行

因此需要新增 **Auto Entry Robot 子系统**。

------------------------------------------------------------------------

# 二、系统整体架构

系统分为五层：

    Blockchain
        ↓
    Data Listener
        ↓
    Feature Engine
        ↓
    Scoring Engine
        ↓
    Robot Engine
        ↓
    Execution Engine

详细结构：

    Chain Listener
    监听链上事件

    Aggregator
    交易数据聚合

    Feature Extractor
    指标计算

    Scoring Engine
    评分计算

    Robot Planner
    策略决策

    Execution Service
    自动交易执行

    Dashboard
    可视化界面

------------------------------------------------------------------------

# 三、技术架构（Python核心）

系统技术结构：

前端\
Dashboard、参数配置、信号展示

Python Core\
监听、指标计算、策略引擎、交易执行

数据库\
存储交易、评分、特征数据

推荐技术栈：

-   Python 3.11+
-   FastAPI
-   PostgreSQL
-   Redis
-   web3.py
-   asyncio

------------------------------------------------------------------------

# 四、Python核心模块设计

## 1 Listener 监听层

负责接收链上事件

监听事件：

-   PairCreated
-   Swap
-   Transfer
-   Mint
-   Burn

技术：

-   asyncio
-   websocket RPC
-   web3.py

------------------------------------------------------------------------

## 2 Pool Discovery 模块

自动识别流动性池

识别方式：

监听 Factory

    PairCreated(token0, token1, pair)

补查机制：

    getPair(token, WETH)
    getPair(token, USDC)
    getPair(token, VIRTUAL)

------------------------------------------------------------------------

## 3 Aggregator 聚合层

负责时间窗口聚合

窗口：

-   30秒
-   60秒
-   3分钟

输出：

-   交易数量
-   买卖数量
-   成交额
-   独立买家数

------------------------------------------------------------------------

## 4 Feature Engine 特征工程

将聚合数据转换为特征

示例特征：

-   buy_sell_ratio
-   unique_buyers_growth
-   large_order_ratio
-   abnormal_sell_pressure
-   breakeven_gap_ratio

------------------------------------------------------------------------

## 5 Scoring Engine 评分系统

评分类型：

-   heat_score
-   structure_score
-   phase_score
-   risk_score

总评分：

    entry_score =
    0.45 * heat_score +
    0.30 * structure_score +
    0.15 * phase_score -
    0.20 * risk_score

评分区间：

  分数    行为
  ------- ------
  0‑55    观察
  55‑70   候选
  70‑82   试单
  82+     加仓

------------------------------------------------------------------------

# 五、机器人策略设计

三段式入场策略

### 试单

条件：

entry_score ≥ 70

执行：

买入20%计划仓位

### 确认加仓

条件：

entry_score ≥ 82

执行：

追加30%仓位

### 趋势加仓

条件：

持续上涨且成交量增加

执行：

买入剩余仓位

------------------------------------------------------------------------

# 六、风险控制

风险控制规则：

单项目最大仓位

    ≤ 总资金 20%

单次试单

    ≤ 总资金 5%

止损

    亏损10%自动卖出

暂停规则

连续亏损3次暂停交易

------------------------------------------------------------------------

# 七、数据库结构

## token_pools

    id
    token_address
    pool_address
    factory_address
    quote_token
    is_primary
    discovered_at

## pool_snapshots

    id
    pool_address
    liquidity
    volume_1m
    tx_count_1m
    unique_traders
    timestamp

## feature_snapshots

    id
    token_address
    tx_count_30s
    buy_sell_ratio
    volume_quote_60s
    unique_buyers
    large_order_ratio
    timestamp

## score_snapshots

    id
    token_address
    heat_score
    structure_score
    phase_score
    risk_score
    entry_score
    timestamp

## robot_trades

    id
    token_address
    action
    price
    amount
    reason
    tx_hash
    timestamp

------------------------------------------------------------------------

# 八、运行模式

系统支持三种模式

观察模式

仅计算指标

模拟模式

生成交易但不执行

实盘模式

自动执行交易

------------------------------------------------------------------------

# 九、开发阶段

阶段一

Pool Discovery

阶段二

Feature Engine

阶段三

Scoring Engine

阶段四

Paper Trading

阶段五

Execution Engine

------------------------------------------------------------------------

# 十、未来扩展

未来可扩展为：

-   多链支持
-   策略市场
-   自动回测系统
-   AI策略优化
