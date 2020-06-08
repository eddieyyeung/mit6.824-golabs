# mit6.824-golabs

## Raft
- [Leader Election](#leader-election)
- [Log Replication](#log-replication)

**issues**:
- [ ] `TestBackup2B` 花了一分多钟的时间，需要优化一下
- [ ] 同步消息给状态机的时候可能出现次序不一致的情况。
  猜测是因为当前设计的方案是：`commitIndex` 更改的时候就会触发消息同步，每个消息的发送是由一个协程来操作，这样就有可能导致数据更改频率过快的话，次序有可能打乱。后续应该维护一个协程来同步消息。

### Leader Election
**2020-06-08**

2A 测试情况如下：
```shell
Test (2A): initial election ...
  ... Passed --   3.5  3   32    8180    0
Test (2A): election after network failure ...
  ... Passed --   5.6  3   38    7236    0
PASS
ok      _/mit6.824-golabs/src/raft     9.120s
```
---

### Log Replication
**2020-06-08**

相比于 `lab2A` 分支稍微做了改进。

`lab2A` 中的 leader 采取统一的频率来向其他 servers 发送心跳（日志条目）。

`lab2B` 中的 leader 可以根据不同情况通过不同的频率对其他 servers 发送心跳或日志条目，即发送 `AppendEntries` RPC 请求。假如某一个 server 中的 `prevLogIndex` 的日志条目没有被同步到，leader 则会加快发送频率早点找出符合条件的 `prevLogIndex`。如果是心跳模式或者正常同步日志条目，频率是 200ms；如果是需要快速找出 `prevLogIndex`，频率是 20ms。

```shell
Test (2B): basic agreement ...
  ... Passed --   1.9  3   16    4166    3
Test (2B): RPC byte count ...
  ... Passed --   4.9  3   46  112634   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   7.6  3   67   15554    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   4.4  5   83   16564    3
Test (2B): concurrent Start()s ...
  ... Passed --   1.7  3   12    3162    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   8.7  3   58   13776    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  63.3  5 1121  315791  104
Test (2B): RPC counts aren't too high ...
  ... Passed --   3.7  3   25    6452   12
PASS
ok      _/mit6.824-golabs/src/raft     97.480s
```

