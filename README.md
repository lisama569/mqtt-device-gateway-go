# 本地 IoT 设备接入网关（Go，零外部依赖）

一个完全在本地运行的 MQTT 风格设备接入网关：TCP 服务端 + Go 客户端 SDK，
不依赖任何外部 broker、第三方 Go 模块或网络资源。用于演示并验证设备接入、
会话归属、主题订阅、QoS 1 可靠投递、离线补发、遗嘱消息与异常链路隔离。

## 能力一览

| 主题 | 确定的行为 |
| --- | --- |
| 会话类型 | `cleanStart=true` 临时会话（连接结束即销毁）；`false` 持久会话（订阅、在途窗口、离线队列跨连接保留） |
| 同标识重复接入 | 新 CONNECT 原子替换属主：先注册新属主 → 发出新 CONNACK → 再关闭旧连接；旧连接的遗嘱**不**发布；任意时刻一个 clientID 最多一条连接持有会话 |
| 主题过滤 | 支持 `+`（单层）与 `#`（多层，只能结尾）；空/非法过滤条件、空主题名、通配符出现在主题名中一律拒绝，不静默接受 |
| QoS | 支持 QoS 0/1；QoS 2 在 CONNECT/PUBLISH/SUBSCRIBE 各处**明确拒绝**，绝不悄悄降级 |
| QoS 1 投递 | 每会话在途窗口（默认 16）；报文标识在窗口内不重复；PUBACK 释放槽位；重连用同一标识、`DUP=1` 重传在途报文 |
| 重复报文 | 入站 QoS1 标识进入有界去重窗口（默认 64）：重复发送会再次收到 PUBACK，但消息**不会二次投递**；对未知/重复 PUBACK 只记录不动作 |
| 离线补发 | 持久会话离线期间的 QoS1 消息按 FIFO 进入有界队列（默认 1000），重连后先补发离线队列、再 `DUP=1` 重传旧在途报文；不重不丢（队列未满时） |
| 溢出策略 | 离线队列满时丢弃**最旧**消息并计数，日志记录被丢弃与保留的主题 |
| 遗嘱（Will） | 仅在**异常断开**（链路错误、协议错误、keepalive 超时）时发布**恰好一次**；正常 DISCONNECT 与被新连接接管时**不**发布；网关整体关闭也不发布 |
| 异常隔离 | 单连接的畸形/超大报文只关闭该连接；不影响其他会话；注册表按实例身份注销，旧连接无法清理新属主的会话 |
| Keepalive | CONNECT 声明秒数，服务端按 1.5 倍超时判定失活并按异常断开处理（发遗嘱） |

## 目录结构

```
internal/protocol/  线协议：长度前缀 JSON 帧、报文类型、错误分类、主题校验/通配符
internal/session/   每设备会话状态：订阅表、在途窗口、离线队列、入站去重
internal/broker/    TCP 网关：连接生命周期、接管、路由、遗嘱、keepalive 看门狗
client/             Go 设备客户端 SDK（Connect/Subscribe/Publish QoS1/Disconnect）
cmd/gateway/        本地可启动的网关进程
examples/localdemo/ 零配置端到端演示（离线补发 + 遗嘱）
```

## 线协议（简化但确定）

每个报文 = 4 字节大端长度前缀 + JSON 主体，单帧上限 4 MiB（超限返回
`OVERSIZED_PACKET` 并断开）。报文类型：`CONNECT/CONNACK/PUBLISH/PUBACK/
SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK/PINGREQ/PINGRESP/DISCONNECT`。

CONNECT 必须携带协议名 `LIG` 与版本 `1`；无法识别的协议名/版本返回
CONNACK reason=1 后断开。

## 本地验证

需要 Go 1.25+。

```bash
# 全部自动化测试（协议 / 会话 / 网关集成 / 客户端 SDK）
go test ./...

# 并发与内存竞态检测（建议多跑几遍）
go test -race -count=3 ./...

# 零配置端到端演示：离线顺序补发 + 遗嘱恰好一次
go run ./examples/localdemo

# 启动一个本地网关（默认 127.0.0.1:18830）
go run ./cmd/gateway -addr 127.0.0.1:18830
```

### 客户端 SDK 用法

```go
c, err := client.Connect(client.ConnectConfig{
    Address: "127.0.0.1:18830", ClientID: "sensor-1", CleanStart: false,
}, func(topic string, payload []byte, packetID uint16, dup bool) {
    // 异步收到 PUBLISH；dup=true 表示服务端重传
})
res, err := c.Subscribe([]protocol.SubFilter{{Filter: "home/+/temp", QoS: 1}})
err = c.Publish("home/room-a/temp", 1, []byte("21.5")) // QoS1：阻塞至 PUBACK
err = c.Disconnect()                                   // 正常断开，不发遗嘱
```

模拟设备异常断电（不发 DISCONNECT，触发遗嘱）：

```go
_ = c.KillForDemo()
```

## 日志可解释性

所有判定日志都带 `clientID`、`packetID`（涉及报文时）与 `basis/cause`，例如：

```
WARN duplicate inbound PUBLISH: re-acknowledging without re-delivery clientID=.. packetID=42
INFO QoS1 PUBLISH delivered clientID=.. packetID=7 topic=.. dup=false inflight=3
INFO session take-over: closing previous connection after new CONNACK clientID=..
WARN abnormal disconnect: publishing will message clientID=.. cause=KEEPALIVE_TIMEOUT willTopic=..
WARN spurious/duplicate PUBACK ignored clientID=.. packetID=9 basis="identifier not present in inflight window"
WARN offline queue overflow: oldest message dropped clientID=.. droppedTopic=.. keptTopic=..
```

## 错误分类与 CONNACK reason

详见 [docs/design.md](docs/design.md) 的「错误分类表」。

- reason 1：协议名/版本不被支持
- reason 2：设备标识非法（空或超长）
- reason 3：报文畸形（非法 JSON、空订阅、非法主题/过滤条件等）
- reason 4：QoS 等级不支持
- SUBACK 单过滤条件失败码：`0x80`
