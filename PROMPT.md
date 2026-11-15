# 生成同款 Token 看板的提示词

把下面整段贴给 WorkBuddy，一次对话即可产出同款工具（单文件 Python + 内嵌网页，零依赖）。

```text
做一个本机 WorkBuddy token / 积分监控看板：单文件 Python、零第三方依赖、双击启动、浏览器打开即用。全程自主完成，不要中途向我确认，探测到字段与预期不一致就自己适配后继续。

【先探后写】
读 ~/.workbuddy/projects/*/*.jsonl 最近 3 个各 200 行，打印一条含 rawUsage 的完整结构，实测确认真实字段名：prompt_tokens、completion_tokens、prompt_cache_hit_tokens（或 prompt_tokens_details.cached_tokens）、credit、requestModelName、messageId、stepSeq、conversationRequestId、argumentsDisplayText。同时确认用户提问与模型调用的配对方式（用户消息 _meta.baggage 里的 conversation_request_id ↔ providerData.conversationRequestId）。再检查子 Agent 目录 projects/<项目>/<会话id>/subagents/agent-*.jsonl：其 providerData 多出 agent 字段（Explore、general-purpose 等），且子 Agent 用户消息的 baggage 里带的是父请求 id，必须改用 providerData.conversationRequestId 归属。最后只读打开 ~/.workbuddy/workbuddy.db，核对 sessions 与 session_usage(credit_json) 的实际列名和格式。一切以实测为准。

【采集】
启动即自动扫描本机全部历史会话（projects/*/*.jsonl 与 projects/*/*/subagents/*.jsonl），零配置，子 Agent 用量并入所属会话。后台线程每 1.5 秒增量扫描：记录文件偏移量，只读新增字节。用 (来源, messageId, stepSeq) 去重；子 Agent 的轮次 id 加文件前缀避免与父会话撞号。积分双源校验：jsonl 的 rawUsage.credit 累加 vs db 的 credit_json 按 requestId，同一请求取较大值。db 用 mode=ro 只读连接，失败退 immutable。端口占用自动顺延。

【界面】浅色、四块纵向布局
1. 左栏任务列表：标题、项目、模型、轮次、总 token、积分、最近活跃；带搜索和排序（最近活跃 / Token 总量 / 积分 / 轮次）。
2. 顶部统计卡：模型、状态、轮次数、步骤数、输入/输出/缓存命中的 token、总 token、累计积分、缓存命中率、上下文占用。
3. 折线图：累计 Token / 单步 Token / 累计积分 / 缓存命中率 四指标切换，悬停显示时间与数值，相邻调用间隔超 20 分钟断开折线。
4. 可视化卡：按工具聚合（横向条形，按值降序）与按轮次堆叠（每轮一柱、按工具分色、带图例）两种视图，可切「积分 / 次数」。
5. 明细表：逐轮 / 逐步骤切换。逐轮显示轮次、来源、提问摘要、时间、耗时、工具数、token、积分；逐步骤显示时间、轮次、来源、工具名、动作摘要（argumentsDisplayText，缺失回退正文或 arguments，去空白截 180 字）、token、积分、模型。来源列标出主会话 / 子 Agent 名，并可按来源筛选。每列可点击排序，三态循环降序→升序→取消。支持按工具筛选、导出 CSV。
6. 顶部标注「子 Agent N 个 · X 步 · Y 分」，可视化左侧可切「按工具 / 按来源」。
7. 空态话术齐全：还没有数据 / 正在扫描 / 暂无步骤数据 / 正在载入。

【性能】
前端每 1.8 秒轮询 /api/state?session=<会话>&dv=<版本号>，版本号没变只回统计、不重发明细。首次扫描显示进度条。

【避坑】
- flex 滚动子项必须同时写 min-height:0 和 overflow:hidden，内部再滚动，否则内容撑破卡片压住表头。
- 切任务时旧轮询响应会覆盖新数据：用请求序号丢弃过期响应，并校验 detail.summary.id 与当前选中会话一致，不一致就显示「正在载入」。
- 不做面板拖拽改大小，固定尺寸（侧栏 312px、图表约 326px、可视化约 212px、明细约 340px；宽度 <1300px 时图表与可视化改上下排列）。
- 时长统一：>1h 用 h、>1m 用 m、否则 s。
- 展示前清洗 <user_query> 等标签。
- 全程只读，不写任何文件到 ~/.workbuddy。

【交付】
配套 start.bat 与 start.sh（自动检测 python、启动服务、打开浏览器）。做完实际启动一次，选中真实会话，截图统计卡、折线图、逐步骤明细，并汇报实测到的字段结构和 jsonl 与 db 积分的吻合情况。
```
