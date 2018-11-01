mage-module-shard
=================

Module to help you create distributed MAGE modules. 

Installation
-------------

```shell
npm install --save mage-module-shard
```

Usage
-----

Please take note the following requirments:

1. Methods on objects will **not** be set on the remote node; you
    should stick to sending only data objects

> lib/modules/sharded/index.ts

```typescript
import AbstractShardedModule, {
  IShard
} from 'mage-module-shard'

class ShardedModule extends AbstractShardedModule {
    public async willForwardSomeCalls(state: mage.core.IState) {
      const shard = this.createShard(state.actorId)
      const val = await shard.willRunRemotely(state)

      return { shard, val }
    }

    public async forwardUsingShard(state: mage.core.IState, shardData: IShard) {
      const shard = this.getShard(shardData)
      const val = await shard.willRunRemotely(state)

      return { shard, val }
    }

    public async sendToAll(state: mage.core.IState) {
      const broadcast = this.createBroadcast()
      const [errors, values] = await broadcast.willRunRemotely(state)

      if (errors) {
        throw new Error('failed')
      }

      // A map of hash id to values is returned
      return Object.values(values)
    }

    public willRunRemotely(state: mage.core.IState) {
      return 1
    }
}

export default new ShardedModule()
```

This module works as follow:

  1. Call `this.createShard(string)` to receive a proxy instance; all API calls
     made on this proxy will always be forwarded to the same server
  2. **Keep this proxy around**: Normally, you will pass this to your game client(s)
  3. **Reuse the proxy data in future calls**: Game clients will send you back this data,
    and you will use it to route other related calls to the same MAGE node

Todo
----

- [x] broadcast: send a call to all nodes in the cluster and receive an array of responses
- [ ] set attributes over shard/broadcasts
- [ ] limit: only let a limited number of nodes run this module

License
-------

MIT
