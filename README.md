# 本地 IoT 设备接入网关 (LGW1)

一个**纯本地、无外部依赖**的 IoT 设备接入网关。用 Go 实现：TCP 服务端 + 自定义
长度前缀二进制协议 + Go 设备客户端，模拟设备的接入、订阅、消息投递与异常断连。

- 持久会话 / 临时会话；同设备标识的会话接管（确定规则、不会同时有两份状态）
- 主题通配符订阅与合法性校验（非法/空条件**拒绝**而非静默接受）
- QoS1 可靠投递：报文标识窗口、入站去重、重复/未知 ACK 的确定处理
- 持久会话**离线消息按原顺序补发**，重连后未确认窗口置 Dup 重发（不重不漏）
- 异常断连发布**遗嘱恰好一次**；正常 `DISCONNECT` 与被接管时不发布
- 单 broker 级路由 goroutine，故障隔离：一路设备阻塞/出错不影响其他会话，
  不会死锁、连接泄漏或错投

## 目录结构

```
internal/proto     线协议：长度前缀帧、报文编解码、原因码
internal/topic     主题名校验、+/# 通配符匹配
internal/session   会话状态：订阅、离线 FIFO 队列、65535 报文标识窗口、在途重发
internal/broker    接入、会话接管、QoS0/1 路由、遗嘱、错误分类、异常隔离
internal/client    Go 设备客户端：握手、保活、QoS1 收发、正常/异常断连
cmd/gateway        可启动的 TCP 网关
cmd/demo           端到端演示（全部主要行为）
```

## 运行与本地验证

要求 Go 1.25+（模块 `go 1.25.0`）。

```bash
# 1) 全部单元测试（含并发、异常断连、遗嘱、补发、畸形报文场景）
go test ./...

# 带竞态检测
go test ./... -race

# 2) 端到端演示：通配符投递、拒绝非法订阅、遗嘱、接管、离线有序补发
go run ./cmd/demo

# 3) 启动网关（默认 127.0.0.1:9100）
go run ./cmd/gateway -addr 127.0.0.1:9100
```

## 线协议 LGW1

帧采用小端长度前缀，所有写入都做完整写（防止短写造成字节流错位）：

```
+----------------+----------+--------------------+
| total : uint32 | type : 1 | payload (total-1)  |
+----------------+----------+--------------------+
```

`total` 含类型字节；长度头固定 4 字节。单帧负载上限 1 MiB，超限在读取阶段即按
`payload-too-large` 拒绝并断连，不会继续消费后续字节。

报文类型：`CONNECT(1) CONNACK(2) PUBLISH(3) PUBACK(4) SUBSCRIBE(5) SUBACK(6)
UNSUBSCRIBE(7) UNSUBACK(8) PINGREQ(9) PINGRESP(10) DISCONNECT(11)`。

CONNECT 载荷含协议名 `LGW1`、版本 `1`、CleanSession、KeepAlive、ClientID、
可选遗嘱（主题/QoS/负载）。握手不合法时在 CONNACK 中返回明确原因码后关闭：

| ReasonCode          | 触发情形                                   |
| ------------------- | ------------------------------------------ |
| `unsupported-proto` | 协议名不是 LGW1 或版本不是 1              |
| `bad-identifier`    | ClientID 为空                              |
| `payload-too-large` | 帧长度声明超过 1 MiB                       |
| `qos-not-supported` | 遗嘱或订阅请求 QoS>1（不降级）            |
| `bad-topic-filter`  | 遗嘱主题或订阅过滤条件非法                 |
| `session-takeover`  | 同 ClientID 新连接接入，旧连接收到此通知  |

## 会话与投递语义

### 会话类型
- **临时会话**（CleanSession=true）：仅在连接期间存在；断连即删除订阅与队列；
  离线期间到达的消息直接丢弃。
- **持久会话**（CleanSession=false）：跨连接保留订阅、离线队列和未确认窗口；
  同一 ClientID 任意时刻只有一个连接持有它。

### 会话接管（规则确定、可解释）
新连接用相同 ClientID 接入时**新连接获胜**：旧连接立即收到 `session-takeover`
CONNACK 并被关闭，注册表槽位移交给新连接。旧连接因此**不发布遗嘱**（设备并未
“死亡”，只是换了链路），也无法触碰共享会话。接管通知写与关闭做了限时/解耦处理，
即使对端接收窗口占满也不会阻塞新连接握手。

### 主题与订阅
- 发布主题不允许 `+`/`#`；`+` 匹配单层，`#` 只能作为最后一级。
- SUBSCRIBE 逐个过滤条件返回授权字节：成功为授予 QoS(0/1)，失败为
  `0x80 | ReasonCode`（如 `0x85` = 非法主题）。非法或空条件**逐项拒绝**，
  不会静默接受。UNSUBSCRIBE 对未知过滤条件仍幂等回 UNSUBACK。
- 在线投递的有效 QoS = `min(发布QoS, 订阅QoS)`。

### QoS1 可靠投递
- 报文标识取自 1..65535 的每会话窗口；窗口耗尽时入队被明确拒绝并记录原因，
  最多 65535 条在途。
- **入站去重**：相同报文标识的重复 PUBLISH 只路由一次，但每次都回 PUBACK；
  重复或未知的 PUBACK 记日志后忽略，不改变状态。
- 出站消息先进有序会话队列（Enqueue），发送时移入在途表（PopForSend）；
  ACK 到达才释放标识（AckPending）。
- **重连补发**：新连接建立后，先把在途消息置 `Dup=true` 按报文标识升序重发，
  再按 FIFO 发送离线期间排队的消息——因此既不重复占用、也不乱序、不丢失。
- 离线期间只有 QoS1 会为持久会话排队；QoS0 不存储（尽力而为）。

### 路由与故障隔离
- 所有扇出由**单个 router goroutine** 完成：连接读循环只把不可变消息投入有界
  队列即返回，从不在别的设备 socket 上阻塞，避免成环反压与死锁。
- 每条连接有独立 reader（读帧 + 1.5× 保活判定）与 writer（唯一写 socket）。
  PUBACK/SUBACK 等应答在处理该帧的同一轮里**同步写出**，不会被后续事件饿死；
  QoS1 队列由一个可合并、与边沿无关的 `kick` 提示驱动，丢信号或多信号都不会
  造成消息滞留或重复。
- 一路设备异常只关闭它自己：其它会话的收发不受影响，且不会错投。

### 遗嘱（Last Will）
- 仅在**异常**结束时发布**恰好一次**：协议错误、超大帧、保活超时、TCP EOF/重置。
- 正常 `DISCONNECT` 不发布；被新连接接管也不发布。
- 遗嘱本身走与普通发布相同的 QoS 与匹配路径（可为 QoS1）。

### 错误分类（日志可见判定依据）
连接结束原因在日志中以固定原因串给出：`protocol-error`、`payload-too-large`、
`keepalive-timeout`、`peer-gone`、`clean-disconnect`、`session-takeover`、
`server-shutdown`。日志统一带 `client_id`、`packet_id`、`topic`、`qos`、
`decision`、`reason/code` 等字段，便于解释每一步判定。

## 自动化测试覆盖

- `internal/topic`、`internal/proto`、`internal/session`：匹配规则、往返编解码、
  FIFO/重复 ACK/窗口边界。
- `internal/broker`：
  - `TestConcurrentTakeover`：20 个同标识连接并发接入，断言只有一个存活连接。
  - `TestPersistentOfflineReplay`：异常下线后 4 条 QoS1 重连按 A..D 顺序补发、
    无重复。
  - 遗嘱：异常断开发布一次；正常断开不发布；接管不发布。
  - 畸形/超大帧、首帧非 CONNECT、非法协议版本、空 ClientID、非法订阅、
    QoS2 拒绝、邻居故障隔离。
  - `TestDuplicateInboundPublishRoutedOnce`：相同报文标识重复投递只路由一次、
    每次都应答。
- `internal/client`：断连后 goroutine 无泄漏。
