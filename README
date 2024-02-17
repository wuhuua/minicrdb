# minicrdb

### ![brightgreen](https://img.shields.io/badge/-raft-brightgreen)![important](https://img.shields.io/badge/-all_platform-important)![blueviolet](https://img.shields.io/badge/-based_on_pebble-blueviolet)![informational](https://img.shields.io/badge/-MuLan_Lisense-informational)![red](https://img.shields.io/badge/-English-red)



## Reading and writing keys

The reference implementation is a very simple in-memory key-value store. You can set a key by sending a request to the HTTP bind address (which defaults to `localhost:8080`):

```bash
curl -XPOST localhost:8080/key -d '{"foo": "bar"}'
```

You can read the value for a key like so:

```bash
curl -XGET localhost:8080/key/foo
```

## Running minicrdb

*Building hraftd requires Go 1.20 or later. [gvm](https://github.com/moovweb/gvm) is a great tool for installing and managing your versions of Go.*

Starting and running a minicrdb cluster is easy. 
Build minicrdb like so:

```bash
go build
```

Run your first node:

```bash
./minicrdb start
```

You can now set a key and read its value back:

```bash
curl -XPOST localhost:8080/key -d '{"user1": "batman"}'
curl -XGET localhost:8080/key/user1
```

### Bring up a cluster

Let's bring up 2 more nodes, so we have a 3-node cluster. That way we can tolerate the failure of 1 node:

```bash
./minicrdb start --http-port "8081" --join "localhost:8080" --node-port "13152" --store "./minicrdb_data_2"
./minicrdb start --http-port "8082" --join "localhost:8081" --node-port "13152" --store "./minicrdb_data_3"
```

This tells each new node to join the existing node.
You can join any node in cluster.
Once joined, each node now knows about the key:

```bash
curl -XGET localhost:8081/key/user1
curl -XGET localhost:8082/key/user1
```

Furthermore you can add a second key, and you can set or get on any node even they doesn't have that range

```bash
curl -XPOST localhost:8081/key -d '{"user2": "robin"}'
```

Confirm that the new key has been set like so:

```bash
curl -XGET localhost:8080/key/user2
curl -XGET localhost:8082/key/user2
```
## License

Minicrdb uses [MulanPSL2](https://license.coscl.org.cn/MulanPSL2). Feel free to copy the source code. When editting or delivering, please obey [MulanPSL2](https://license.coscl.org.cn/MulanPSL2).