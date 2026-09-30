# 设计说明：会话、投递语义与错误分类

本文描述本地 IoT 网关在各类边界条件下的**确定性行为与判定依据**，所有结论
均有对应自动化测试覆盖（见文末测试矩阵）。

## 1. 连接与 CONNECT 握手

1. TCP 接受连接后，**第一帧必须是 CONNECT**，且 10 秒内到达，否则断开。
2. CONNECT 校验顺序（任一失败即返回对应 CONNACK reason 并断开）：
   1. 协议名必须为 `LIG`，版本必须为 `1` → 否则 reason=1；
   2. `clientID` 非空且 ≤ 128 字节、不含 NUL → 否则 reason=2；
   3. 遗嘱（若有）的主题合法、QoS ∈ {0,1} → 否则 reason=3/4；
   4. 其他字段的 QoS>1 → reason=4（明确拒绝，不降级）。
3. 握手成功返回 reason=0，之后进入报文循环。

### 1.1 会话归属（同标识重复接入）

这是本系统最重要的不变量：**一个 clientID 在任意时刻最多有一条连接持有
其会话状态**。

- 会话注册表（`session.Store`）与连接注册表（`Broker.conns`）都用互斥锁保护。
- 新 CONNECT 通过 `registerOwner` **原子地**替换连接注册表中的属主，拿到被
  替换的旧连接（但此时不关闭它）。
- 先发完新连接的 CONNACK，再调用 `closeDisplaced` 关闭旧连接——这样即使在
  高并发接管时，客户端也不会把"被接管"观察成"握手失败"。
- 旧连接关闭时带 `takenOver=true`：
  - **不发布遗嘱**（新 CONNECT 显式取代，不是异常死亡）；
  - 不删除会话（删除是"按实例身份"比较的，旧实例也删不掉新会话）。
- 旧连接上任何迟到的写入都会因 `shutdownCh` 关闭/`epoch` 变化而失败。

### 1.2 会话生命周期

| CONNECT 选项 | 已存在持久会话 | 行为 |
| --- | --- | --- |
| `cleanStart=true` | 任意 | 丢弃旧会话（在途/队列/订阅全部放弃），新建临时会话 |
| `cleanStart=false` | 有持久会话 | **恢复**同一实例（`created=false`） |
| `cleanStart=false` | 无（或只剩未清理的临时会话） | 新建持久会话 |

临时会话在连接终止（正常或异常）后从 Store 删除；持久会话始终保留。

## 2. 订阅与主题匹配

- 层级分隔符 `/`；`+` 匹配且仅匹配一个层级；`#` 只能作为最后一个层级，匹配
  零个或多个后续层级（因此 `a/#` 也匹配 `a`）。
- PUBLISH 主题名中禁止出现 `+`/`#`；空主题、NUL 字节一律拒绝。
- SUBSCRIBE 必须带非零报文标识、非空过滤条件列表；每个过滤条件单独校验：
  - 非法过滤条件（如 `a/#/b`、`a/+b`）→ 整个操作作为协议错误处理并断开，
    **不会静默订阅一个子集**；
  - 过滤条件合法但 QoS>1 → 该条件在 SUBACK 中返回 `0x80`，其他条件正常授予。
- 实际投递 QoS = min(发布 QoS, 订阅授予 QoS)（例如 QoS1 消息发给 QoS0
  订阅者，按 QoS0 投递；这是协议规定的降级，而非"悄悄丢弃"）。

## 3. 可靠投递（QoS 1）

### 3.1 出站方向（broker → 设备）

- 每个会话一个有界**在途窗口**（`MaxInflight=16`）和一个 FIFO **待发队列**。
- 报文标识由会话单调分配（1..65535，跳过窗口内仍占用的标识，绕回后从 1
  重新开始，0 永不使用）。
- 窗口满时，新消息留在待发队列队尾，不占用标识；收到 PUBACK 释放槽位后
  由 `pumpSession` 继续发送。
- PUBACK 的判定：
  - 标识在窗口中 → 移除并继续泵队列；
  - 标识不在窗口中（迟到/重复/伪造）→ 记录 `spurious/duplicate PUBACK`，
    **不**影响任何其他报文。
- 发送时底层写失败：报文保留在窗口中，连接进入异常终止；重连后按 3.3 重传。

### 3.2 入站方向（设备 → broker）

- QoS1 PUBLISH 必须带非零标识，否则作为畸形报文拒绝。
- 入站标识进入有界去重环形窗口（`InboundDupWindow=64`）：
  - 首次出现：正常路由，回 PUBACK；
  - 窗口内再次出现（设备重发）：**再回一次 PUBACK，但不重复路由/投递**；
  - 标识滑出窗口后可被合规客户端重新使用，此时视为新报文。

### 3.3 离线与重连补发

- 路由时目标会话**离线**：
  - 有效 QoS 1 + 持久会话 → 进入该会话的有界离线队列；
  - 有效 QoS 0 → 丢弃（QoS0 不存储）；
  - 临时会话 → 丢弃（临时会话没有离线存储）。
- 离线队列上限 `MaxOfflineQueue=1000`；满后采用**最旧优先丢弃**，丢弃计数
  可通过 `Session.OverflowDropped()` 观察，日志记录被丢弃主题与新保留主题。
- 持久会话重连后的发送顺序固定为：
  1. `DrainOffline()`：离线队列按入队顺序整体搬入待发队列；
  2. 断开前仍在途的报文：用**同一报文标识 + `DUP=true`** 重传；
  3. `pumpSession()`：发送待发队列（离线补发 + 新到消息），维持每会话 FIFO。
- 遗嘱消息走同一条路由路径，因此离线持久订阅者也能在重连后收到遗嘱。

## 4. 断开与遗嘱

终止处理集中在 `conn.terminate`，通过 `atomic.Bool` 保证**恰好执行一次**。

| 触发原因 | normal | takenOver | 会话处理 | 遗嘱 |
| --- | --- | --- | --- | --- |
| 收到 DISCONNECT | true | false | 持久保留 / 临时删除 | **不发** |
| 新 CONNECT 接管 | false | true | 归属不变 | **不发** |
| 链路读错误/EOF | false | false | 持久保留 / 临时删除 | 发，恰好一次 |
| 协议错误（畸形报文等） | false | false | 同上 | 发，恰好一次 |
| keepalive 1.5 倍超时 | false | false | 同上 | 发，恰好一次 |
| 握手前失败 | false | false | 无会话/无属主 | 无遗嘱可发 |
| 网关整体 Close | true | false | 状态不动 | **不发**（有序停机） |

遗嘱在注销连接属主**之后**再发布，保证死亡设备不会收到自己的遗嘱。

## 5. 并发、死锁与泄漏防护

- **注册表锁与连接关闭分离**：`registerOwner` 只在持锁期间做 map 替换；
  关闭旧连接（可能阻塞）在锁外执行，杜绝"持注册表锁等 IO"。
- **注销按实例身份**：`unregister`/`Store.Delete` 只在 map 中仍是同一指针时
  删除，被接管的旧连接无法误删新连接的会话。
- **写串行化**：每连接一个容量为 1 的 token channel，所有帧写入互斥；
  `shutdownCh` 关闭后等待写 token 的 goroutine 立即解除阻塞。
- **keepalive 看门狗不自死锁**：看门狗检测到超时后在独立 goroutine 调用
  `terminate` 并立刻退出自身；`terminate` 可以安全地 `Wait` 看门狗退出。
  看门狗在连接进入注册表**之前**启动，避免与 `Broker.Close()` 竞争初始化。
- **路由无全局锁**：投递时先快照在线连接、再逐个会话独立加锁；单个目标
  发送失败只终止它自己的连接。
- **帧大小有界**：4 MiB 上限，超限直接判定并断开，不无限读入内存。

## 6. 错误分类表

| ErrorCode | 触发场景 | 对连接的处理 |
| --- | --- | --- |
| `MALFORMED_PACKET` | 长度为 0、JSON 非法、缺类型、首帧非 CONNECT、QoS1 无标识、非法 SUBSCRIBE 等 | CONNACK reason=3（握手前）或异常断开 |
| `OVERSIZED_PACKET` | 帧长 > 4 MiB | 拒绝并断开 |
| `UNSUPPORTED_PROTOCOL` / `UNSUPPORTED_PROTOCOL_VERSION` | 协议名非 `LIG` 或版本非 1 | CONNACK reason=1 后断开 |
| `INVALID_CLIENT_ID` | 空/超长/含 NUL | CONNACK reason=2 |
| `INVALID_TOPIC` / `INVALID_SUBSCRIPTION` | 主题名/过滤条件非法 | reason=3，异常断开 |
| `QOS_NOT_SUPPORTED` | 任何环节出现 QoS 2 | reason=4（CONNECT/PUBLISH）或 SUBACK `0x80`，绝不降级 |
| `KEEPALIVE_TIMEOUT` | 1.5 倍 keepalive 内无任何报文 | 异常断开 + 遗嘱 |
| `CONNECTION_CLOSED` | EOF / reset / 读写失败 | 异常断开 + 遗嘱（若已注册） |

## 7. 自动化测试矩阵

| 测试 | 覆盖点 |
| --- | --- |
| `protocol`: `TestTopicMatch` / `TestInvalidFiltersAndTopics` | 通配符边界、非法输入拒绝 |
| `protocol`: `TestFrameRoundTrip` / `TestReadFrameMalformed` / `TestReadFrameOversized` | 帧编解码、畸形、超大 |
| `session`: `TestInflightWindowAndAck` | 窗口边界、标识不复用、重复/伪造 PUBACK |
| `session`: `TestInboundDedupWindow` | 重复标识判定与窗口滑出复用 |
| `session`: `TestOfflineDrainOrder` | 离线补发 FIFO、不重不丢 |
| `session`: `TestOfflineOverflowDropsOldest` | 最旧优先丢弃与计数 |
| `session`: `TestStoreCreateResumeDelete` | 临时/持久会话创建、恢复、按实例删除 |
| `broker`: `TestConnectAcceptsAndRejects` | 协议名/版本/标识/首帧/畸形 JSON/超大帧 |
| `broker`: `TestConcurrentSameClientIDTakeOver` | 12 路并发同标识接入，属主唯一，旧连接被关 |
| `broker`: `TestPersistentSessionOfflineReplayInOrder` | 离线 5 条按序补发、标识唯一 |
| `broker`: `TestDuplicateInboundQoS1AckedOnce` | 同标识重发：二次 ACK、一次投递 |
| `broker`: `TestQoS2RejectedAndInvalidSubscriptions` | QoS2 拒绝不降级、非法/空订阅拒绝 |
| `broker`: `TestWillOnAbnormalOnlyAndExactlyOnce` | 正常断开/接管不发遗嘱；异常断开恰好一次 |
| `broker`: `TestWildcardRoutingAndFaultIsolation` | 通配符路由；邻居发送垃圾报文不影响他人 |
| `broker`: `TestKeepAliveTimeoutTriggersWill` | keepalive 超时 → 遗嘱，且不重复 |
| `broker`: `TestInflightRetransmitWithDupOnReconnect` | 未确认报文重连同标识 `DUP=1`，不重复 |
| `broker`: `TestEffectiveQoSDowngradeBySubscription` | min(发布,订阅) 的协议级降级 |
| `client`: SDK 用例 | 订阅/退订往返、QoS2 与空订阅客户端侧拒绝、接管关闭旧客户端 |

所有测试可通过 `go test -race ./...` 运行，已在 `-race -count=5` 下无竞态通过。
