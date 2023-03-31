A Zookeeper proxy, built on Rust and Tokio, offers advanced traffic control, auditing, security, and extensibility features. Rely on this robust solution to enhance your Zookeeper experience with improved efficiency and reliability.



## Usage

```
./zookeeper-proxy -c /path/to/conf.toml
```

## example config

```toml
[server]
address = "[::]"
port = 2182

[upstream]
address = "0.0.0.0"
port = 2181


[[circuit_break]]
name = "block getChildren"
regex = "/hello*"
opcode = [8, 12, 1]
```

## supported opcodes


| opcode              | value | supported |
|---------------------|-------|--------|
| CONNECT             | 0     | ✅      |
| CREATE              | 1     | ✅      |
| DELETE              | 2     | ✅      |
| EXISTS              | 3     | ✅      |
| GETDATA             | 4     | ✅      |
| SETDATA             | 5     | ✅      |
| GETACL              | 6     | ✅      |
| SETACL              | 7     | ✅      |
| GETCHILDREN         | 8     | ✅      |
| SYNC                | 9     | ✅      |
| PING                | 11    | ✅      |
| GETCHILDREN2        | 12    | ✅      |
| CHECK               | 13    | ✅      |
| MULTI               | 14    | ✅      |
| CREATE2             | 15    | ✅      |
| RECONFIG            | 16    | ✅      |
| CHECKWATCHES        | 17    | ✅      |
| REMOVEWATCHES       | 18    | ✅      |
| CREATECONTAINER     | 19    | ✅      |
| CREATETTL           | 21    | ✅      |
| MULTIREAD           | 22    | ❌       |
| SETAUTH             | 100   | ✅      |
| SETWATCHES          | 101   | ✅      |
| SASL                | 102   | ❌      |
| GETEPHEMERALS       | 103   | ✅      |
| GETALLCHILDRENNUMBER | 104   | ✅      |
| SETWATCHES2         | 105   | ✅      |
| ADDWATCH            | 106   | ✅      |
| WHOAMI              | 107   | ✅      |
| CREATESESSION       | -10   | ❌      |
| CLOSE               | -11   | ✅      |
