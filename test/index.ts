/* tslint:disable:no-console */

import * as config from 'mage/lib/config'
config.set('logging.server.terminal.channels', 'error')

import * as mage from 'mage'
mage.core.logger.disableChannel('warning')
mage.core.logger.disableChannel('notice')

import * as assert from 'assert'
import {
  Shard,
  ShardedModule,
  createModuleInstances,
  destroyNetwork
} from './utilities'


describe('mage-module-shard', function () {
  let network: any
  let modules: any[]
  let serviceDiscovery: any

  // MAGE sets up uncaughtException hooks, we need to remove them!
  process.removeAllListeners()

  before((callback) => (mage.core as any).loggingService.setup(callback))
  // destroy network
  afterEach(() => destroyNetwork(network))

  // Utility methods
  async function getModule(numberOfNodes: number = 1): Promise<ShardedModule> {
    ({ modules } = await createModuleInstances(numberOfNodes))

    return modules[0]
  }

  async function createShard(numberOfNodes: number = 5): Promise<Shard> {
    await getModule(numberOfNodes)

    const mod = modules[0]
    const remoteModule = modules[numberOfNodes - 1]

    // Get shard data for node3, create a proxy on node1
    const shardData = remoteModule.getLocalShard()
    return mod.getShard(shardData)
  }


  async function expectError(message: string, call: () => Promise<void>) {
    try {
      await call()
    } catch (error) {
      assert.strictEqual(error.message, message)
      return
    }

    throw new Error('Call should have failed')
  }

  /**
   * Custom instanciation
   */
  describe('Module instanciation', function () {
    it('Allows to set a custom module name', function () {
      const mod = new ShardedModule('mymodule')

      assert.strictEqual(mod.name, 'mymodule')
    })
  })

  /**
   * Instances registration over network
   */
  describe('node register/unregister', function () {
    it('register nodes works properly', async function () {
      ({ network, modules } = await createModuleInstances(2))

      for (const mod of modules) {
        assert.strictEqual(mod.clusterSize, 2)
      }
    })

    it('unregister nodes works properly', async function () {
      ({ network, modules, serviceDiscovery } = await createModuleInstances(2))

      const mod = modules[0]
      serviceDiscovery.emit('down', serviceDiscovery.services[1])

      assert.strictEqual(mod.clusterSize, 1)
    })
  })

  /**
   * Get local shard
   */
  describe('getLocalShard', function () {
    it('Shard contains a hash representing the local server instance', async function () {
      const mod = await getModule()
      const shard = mod.getLocalShard()

      assert.strictEqual(shard.id, (mod as any).localNodeHash)
    })
  })

  /**
   * Create shards, and use them to make remote calls
   */
  describe('createShard', function () {
    describe('Can make a remote call', function () {
      it('mmrp send errors are thrown', async function () {
        ({ modules, serviceDiscovery } = await createModuleInstances(5))

        const mod = modules[0]
        const mmrpNode = mod.getMmrpNode()

        mod.getMmrpNode = function () {
          return Object.assign(mmrpNode, {
            send(_requestEnvelope: any, _attempts: number, callback: (error: Error) => void) {
              callback(new Error('whoops mmrp error'))
            }
          })
        }

        // Get shard data for node3, create a proxy on node1
        const shardData = modules[2].getLocalShard()
        const shard = mod.getShard(shardData)

        await expectError('whoops mmrp error', () => shard.methodWithNoArguments())
      })

      it('Throws if the remote node disappeared', async function () {
        ({ modules, serviceDiscovery } = await createModuleInstances(5))

        // Calling from node1
        const mod = modules[0]
        const remoteModule = modules[4]

        // Get shard data for remote node, create a proxy on node1
        const shardData = remoteModule.getLocalShard()
        const shard = mod.getShard(shardData)

        // We cheat a bit here - instead of finding the fully fledged service,
        // we just find the address and create a mock instance of the service, which
        // we will be using to unregister the node everywhere
        const pseudoService = {
          data: mod.clusterAddressMap[shard.id]
        }

        // Drop node1 from list, and close its connection
        serviceDiscovery.emit('down', pseudoService)

        await expectError('Remote node is no longer available', () => shard.methodWithNoArguments())
      })

      it('Throws if the remote node is not connected (timeout)', async function () {
        ({ modules, serviceDiscovery } = await createModuleInstances(5))

        const mod = modules[0]
        const remoteModule = modules[4]

        // Get shard data for node3, create a proxy on node1
        const shardData = remoteModule.getLocalShard()
        const shard = mod.getShard(shardData)

        // Close the remote node's connection, but don't announce it as down
        remoteModule.getMmrpNode().close()

        await expectError('Request timed out', () => shard.methodWithNoArguments())
      })

      it('Non-exising calls throw', async function () {
        const shard = await createShard()
        await expectError('shard.doesNotExist is not a function', () => (<any> shard).doesNotExist())
      })

      it('Remote errors are thrown locally', async function () {
        const shard = await createShard()
        await expectError('I say what what', () => (<any> shard).throwsErrors())
      })

      it('Local calls are not sent over network', async function () {
        const mod = await getModule(5) as any
        const shardData = mod.getLocalShard()
        const shard = mod.getShard(shardData)

        mod.addPendingRequest = () => { throw new Error('Request forwarded over network') }

        const ret = await shard.methodWithNoArguments()

        assert.strictEqual(ret, 1)
      })

      it('No  arguments', async function () {
        const shard = await createShard()
        const ret = await shard.attribute

        assert.strictEqual(ret, 1)
      })

      it('No  arguments', async function () {
        const shard = await createShard()
        const ret = await shard.methodWithNoArguments()

        assert.strictEqual(ret, 1)
      })

      it('Scalar arguments', async function () {
        const shard = await createShard()
        const ret = await shard.methodWithScalarArguments('test', 3)

        assert.strictEqual(ret, 'test,test,test')
      })

      it('Object arguments', async function () {
        const shard = await createShard()
        const ret = await shard.methodWithObjectArguments('world', { hello: 'you'})

        assert.deepStrictEqual(ret, { hello: 'world' })
      })

      it('Array arguments', async function () {
        const shard = await createShard()
        const ret = await shard.methodWithArrayArguments('test', ['hello'])

        assert.deepStrictEqual(ret, ['hello', 'test'])
      })

      it('Promises', async function () {
        const shard = await createShard()
        const ret = await shard.methodWithAsync('test')

        assert.deepStrictEqual(ret, 'test')
      })
    })
  })

  /**
   * Retrieve a shard previously created by createShard
   */
  describe('getShard', function () {
    it('Returns the same shard', async function () {
      const mod = await getModule(5)
      const shard = mod.createShard('test2') // Should hit node2
      const copy = mod.getShard(shard)

      const ret1 = await shard.getModuleId()
      const ret2 = await copy.getModuleId()

      assert.strictEqual(ret1, ret2)
    })

    it('Returns the same shard even if topology changed', async function () {
      ({ network, modules, serviceDiscovery } = await createModuleInstances(5))

      // Calling from node0
      const mod = modules[0]

      // Should hit node2
      const shard = mod.createShard('test2')

      // Find an unrelated node in the cluster
      const index = mod.addressHashes.indexOf(shard.id)
      const randomHash = mod.addressHashes.splice(1, index).shift()

      // We cheat a bit here - instead of finding the fully fledged service,
      // we just find the address and create a mock instance of the service, which
      // we will be using to unregister the node everywhere
      const pseudoService = {
        data: mod.clusterAddressMap[randomHash]
      }

      // Drop an unrelated node from the list
      serviceDiscovery.emit('down', pseudoService)

      // Should still hit node2
      const copy = mod.getShard(shard)

      const ret1 = await shard.getModuleId()
      const ret2 = await copy.getModuleId()

      assert.strictEqual(ret1, ret2)
    })
  })

  /**
   * Make sure shard information remains accessible
   */
  describe('shard enumeration, keys extraction, etc', function () {
    let mod: any
    let shard: any

    beforeEach(async () => {
      mod = await getModule(1)
      shard = mod.createShard('test2')
    })

    it('Returns an IShard when serialized/deserialized (needed for sharing with remotes)', async function () {
      assert.strictEqual(JSON.stringify(shard), `{"id":"${shard.id}"}`)
    })

    it('Object.getOwnPropertyNames', function () {
      assert.deepStrictEqual(Object.getOwnPropertyNames(shard), ['id'])
      // assert.deepStrictEqual(Object.keys(shard), ['id']) // This does not work :(
    })

    it('in operator', function () {
      assert('id' in shard)
    })

    it ('inspect (for util.inspect, REPL, etc)', function () {
      assert.deepStrictEqual(shard.inspect(), { id: shard.id })
    })
  })

  describe('createBroadcast', function () {
    it('can broadcast to all modules', async function () {
      const nodeCount = 3
      const mod = await getModule(nodeCount)
      const broadcast = mod.createBroadcast()
      const [errors, responses] = await broadcast.methodWithScalarArguments('test', 1)
      const values = Object.values(responses)

      assert.strictEqual(errors, null)
      assert.strictEqual(values.length, nodeCount)

      for (const response of values) {
        assert.strictEqual(response, 'test')
      }
    })

    it('can retrieve attributes on all modules', async function () {
      const nodeCount = 3
      const { modules: mods } = await createModuleInstances(nodeCount)
      for (const [pos, mod] of mods.entries()) {
        mod.attribute = pos
      }

      const broadcast = mods[0].createBroadcast()
      const [errors, responses] = await broadcast.attribute
      const values = Object.values(responses)

      assert.strictEqual(errors, null)
      assert.strictEqual(values.length, nodeCount)

      for (const { attribute, localNodeHash: id } of mods as any) {
        assert.strictEqual(attribute, responses[id])
      }
    })

    it('errors are returned in an object', async function () {
      const nodeCount = 3
      const { modules: [mod, remoteMod] } = await createModuleInstances(nodeCount)
      remoteMod.throws = true

      const broadcast = mod.createBroadcast()
      const [errors, responses] = await broadcast.methodThatThrows('test')

      const [error] = Object.values(errors)
      const values = Object.values(responses)

      assert.strictEqual(error.message, `I don't like you`)
      assert.strictEqual(values.length, nodeCount - 1)

      for (const response of values) {
        assert.strictEqual(response, 'test')
      }
    })

    it('error when fetching attribute values are properly managed', async function () {
      const nodeCount = 3
      const { modules: [mod, remoteMod] } = await createModuleInstances(nodeCount)
      remoteMod.throws = true

      const broadcast = mod.createBroadcast()
      const [errors, responses] = await broadcast.mightThrow

      const [error] = Object.values(errors)
      const values = Object.values(responses)

      assert.strictEqual(error.message, `I don't like you`)
      assert.strictEqual(values.length, nodeCount - 1)

      for (const response of values) {
        assert.strictEqual(response, 1)
      }
    })
  })

  /**
   * Internal error handling and checks
   */
  describe('other errors (requests handling)', function () {
    let mod: any

    beforeEach(async () => {
      mod = await getModule(1)
    })

    describe('onRequest', function () {
      it('throws if the method name is missing', async function () {
        await expectError('Method name is missing', () => mod.onRequest([]))
      })

      it('throws if the method is not found', async function () {
        const message = 'Method is not locally available (requested method: does not exist)'
        await expectError(message, () => mod.onRequest(['does not exist', '[]']))
      })
    })

    describe('getPendingRequest', function () {
      it('throws if no request key matches the request id', async function () {
        const message = 'Key not found in request key map (id: doesNotExist)'
        await expectError(message, () => mod.getPendingRequest('doesNotExist'))
      })

      it('throws if no requests are found', async function () {
        mod.pendingRequestsKeyMap.exist = {
          id: 'notexist',
          timestamp: 0
        }
        const message = 'Pending request not found (id: notexist, timestamp: 0)'
        await expectError(message, () => mod.getPendingRequest('exist'))
      })
    })

    describe('deletePendingRequest', function () {
      it('throws if no request key matches the request id', async function () {
        const message = 'Key not found in request key map (id: doesNotExist)'
        await expectError(message, () => mod.deletePendingRequest('doesNotExist'))
      })
    })
  })
})
