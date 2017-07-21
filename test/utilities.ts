/* tslint:disable:no-console */
/* tslint:disable:prefer-function-over-method */
import * as events from 'events'
import * as mage from 'mage'
import AbstractShardedModule from '../src'

const promisify = require('es6-promisify')

const {
  ServiceNode
} = require('mage/lib/serviceDiscovery/node')

const {
  MmrpNode
} = require('mage/lib/msgServer/mmrp')

/**
 * Unless properly initialized, MAGE does not expose MMRP;
 * we force-expose it here for testing purposes
 */
Object.assign(mage.core.msgServer, {
  mmrp: {
    Envelope: require('mage/lib/msgServer/mmrp/Envelope')
  }
})

/**
 * Unless set up, the logger module is not created;
 * we force-create a logger instance here to avoid having to
 * run a full MAGE setup.
 */
Object.assign(mage, {
  logger: mage.core.logger.context('test:unit')
})

/**
 * Mock discovery service
 */
export class TestDiscovery extends events.EventEmitter {
  public name: string
  public type: string
  public services: any[] = []

  private count: number = 0

  constructor(name: string, type: string) {
    super()

    this.name = name
    this.type = type
  }

  public announce(port: number, metadata: any, callback: () => void) {
    this.count += 1

    const service = new ServiceNode(`localhost-${this.count}`, port, [`127.0.0.${this.count}`], metadata)
    this.services.push(service)
    this.emit('up', service)

    callback()
  }

  public discover() {
    for (const service of this.services) {
      this.emit('up', service)
    }
  }

  public close () {
    console.log('closing service discovery')
  }
}

/**
 * MMRP network creation and cleanup utility functions
 */
function createUri(port: number) {
  return `tcp://localhost:${port}`
}

function createNetwork(clientCount: number) {
  clientCount = clientCount || 5

  const result = {
    clientCount,
    relay: null,
    clients: new Array(clientCount)
  }

  result.relay = new MmrpNode('relay', { host: '127.0.0.1', port: '*' }, 'shardTest')

  for (let i = 0; i < clientCount; i += 1) {
    result.clients[i] = (new MmrpNode('client', { host: '127.0.0.1', port: '*' }, `shardTest`))
  }

  return result
}

async function meshNetwork(network: any) {
  const promise = new Promise((resolve) => {
    let count = 0

    network.relay.on('handshake', () => {
      count += 1

      if (count === network.clientCount) {
        resolve()
      }
    })
  })

  for (const client of network.clients) {
    client.relayUp(createUri(network.relay.routerPort), network.relay.clusterId)
  }

  network.relay.relayUp(createUri(network.relay.routerPort), network.relay.clusterId)

  return promise
}

export function destroyNetwork(network: any) {
  if (!network || !network.relay) {
    return
  }

  network.relay.close()
  network.relay = null

  network.clients.forEach(function (client: any) {
    client.close()
  })
}

/**
 * Test module
 */
export class ShardedModule extends AbstractShardedModule {
  public throwsErrors() {
    throw new Error('I say what what')
  }

  public async methodWithNoArguments() {
    return '1'
  }

  public async methodWithScalarArguments(a: string, b: number) {
    const list = new Array(b)

    return list.fill(a).join(',')
  }

  public async methodWithObjectArguments(a: string, b: any) {
    b.hello = a

    return b
  }

  public async methodWithArrayArguments(a: string, b: string[]) {
    b.push(a)

    return b
  }

  public async getModuleId(this: any) {
    return this.getMmrpNode().identity
  }
}

/**
 * Create a number of module instances
 *
 * The instances will be meshed together, and
 * service discovery should trigger so that each node
 * may create its own address map
 */
export async function createModuleInstances(count: number) {
  const network = createNetwork(count)
  const modules = new Array(count)
  const serviceDiscovery =  new TestDiscovery('whatever', 'tcp')

  for (let i = 0; i < count; i += 1) {
    const module = new ShardedModule()
    const untypedModule = <any> module
    const mmrpNode = network.clients[i]

    // Manually inject the MMRP node for this module instance
    untypedModule.getMmrpNode = function () {
      return mmrpNode
    }

    // Inject our fake service discovery system
    untypedModule.getServiceDiscovery = function () {
      return {
        createService() {
          return serviceDiscovery
        }
      }
    }

    // All tests will be running on the same PID; since
    // the PID is used to generate a pseudo-port, we need
    // to mock that as well
    untypedModule.getPseudoPort = function () {
      return i
    }

    // Schedule garbage collection to happen faster;
    // this will be what will trigger the timeout
    untypedModule.scheduleGarbageCollection(50)

    const setup = promisify(module.setup, module)
    await setup(new mage.core.State())

    modules[i] = module
  }

  await meshNetwork(network)

  return {
    network,
    modules,
    serviceDiscovery
  }
}
