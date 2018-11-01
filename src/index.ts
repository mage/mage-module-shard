import * as cluster from 'cluster'
import * as crypto from 'crypto'
import * as mage from 'mage'

const {
  msgServer,
  serviceDiscovery
} = mage.core

const isFunction = require('is-function-x')
const errorToObject = require('serialize-error')
const shortid = require('shortid')

/**
 * This type is not exposed at the moment by MAGE, so
 * we imported its definition back into this project.
 */
type MmrpEnvelopeMessage = string | Buffer


/**
 * Shard module attribute mapping
 */
export type ShardAttribute<T> =
  T extends (...args: infer P) => Promise<infer R> ? (...args: P) => Promise<R> :
  T extends (...args: infer P) => infer R ? (...args: P) => Promise<R> :
  Promise<T>

/**
 * Shard proxy type returned with module.createShard()
 */
export type Shard<T extends AbstractShardedModule> = IShard & {
  readonly [A in keyof T]: ShardAttribute<T[A]>
}

/**
 * Broadcast module attribute mapping
 */
export type BroadcastAttribute<T> =
  T extends (...args: infer P) => Promise<infer S> ? (...args: P) => Promise<[Error[], S[]]> :
  T extends (...args: infer P) => infer R ? (...args: P) => Promise<[Error[], R[]]> :
  Promise<[Error[], T[]]>

/**
 * Broadcast proxy type returned with module.createBroadcast()
 */
export type Broadcast<T extends AbstractShardedModule> = {
  readonly [A in keyof T]: BroadcastAttribute<T[A]>
}

/**
 * IShardedRequestMeta
 *
 * IShardedRequestMeta are used as keys to access ShardedRequest
 * instances stored in a map; they contain the information required to identify
 * the related request, and a timestamp used to determine whether a request
 * has timed out or not.
 *
 * @export
 * @interface IShardedRequestMeta
 */
export interface IShardedRequestMeta {
  id: string
  timestamp: number

}

/**
 * RemoteError class
 *
 * RemoteErrors encapsulate an error that occured
 * either while sending a ShardedRequest, or
 * while executing the request on the remote MAGE node.
 *
 * @export
 * @class RemoteError
 */
export class RemoteError extends Error {
  constructor(data: any) {
    super(data.message)
    Object.assign(this, data)
    this.name = 'RemoteError'
  }
}

/**
 * ShardedRequests
 *
 * @export
 * @class ShardedRequest
 */
export class ShardedRequest {
  /**
   * Request ID
   *
   * This ID is used by the remote server when sending back a response,
   * and by the local server to route the response back to the correct ShardedRequest
   * instance.
   *
   * @type {string}
   * @memberof ShardedRequest
   */
  public id: string

  /**
   * The mmrpNode instance to use to send the message
   *
   * @type {any}
   * @memberof ShardedRequest
   */
  public mmrpNode: any

  /**
   * The event name
   *
   * Should normally be `AbstractShardedModule.REQUEST_EVENT_NAME`
   * for the class which will instanciate it
   *
   * @type {string}
   * @memberof ShardedRequest
   */
  public eventName: string

  /**
   * Target MAGE node
   *
   * @type {string[]}
   * @memberof ShardedRequest
   */
  public target: string[]

  /**
   * Method to be executed
   *
   * @type {string}
   * @memberof ShardedRequest
   */
  public method: string

  /**
   * Arguments to feed to the method
   *
   * @type {any[]}
   * @memberof ShardedRequest
   */
  public args?: any[]

  /**
   * Resolve function (see `send`)
   *
   * @memberof ShardedRequest
   */
  public resolve: (data: any) => void

  /**
   * Reject function (see `send`)
   *
   * @memberof ShardedRequest
   */
  public reject: (error: Error) => void

  constructor(id: string, mmrpNode: any, eventName: string, target: string[], method: string, args?: any[]) {
    this.id = id
    this.mmrpNode = mmrpNode
    this.eventName = eventName
    this.target = target
    this.method = method
    this.args = args
  }

  /**
   * Send the request
   *
   * Note that the sending module is responsible for listening for the
   * response, and passing it to this request through `resolve` or
   * `reject`.
   *
   * The sending module may also call `reject` should it judge a request
   * has timed out.
   *
   * @returns
   * @memberof ShardedRequest
   */
  public async send() {
    const Envelope = msgServer.mmrp.Envelope
    const messages = [this.id, this.method, JSON.stringify(this.args)]
    const source = [this.mmrpNode.clusterId, this.mmrpNode.identity]
    const requestEnvelope = new Envelope(this.eventName, messages, this.target, source, 'TRACK_ROUTE')

    return new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject

      this.mmrpNode.send(requestEnvelope, 1, (error?: Error) => {
        if (error) {
          return reject(error)
        }
      })
    })
  }
}

/**
 * IShard interface
 *
 * This represents the data structure of
 * a shard.
 *
 * @export
 */
export interface IShard {
  id: string
}

function serializeError(data: any) {
  return JSON.stringify(errorToObject(data))
}

/**
 * AbstractShardedModule
 *
 * This abstract module will take care of maintaining
 * a consistent view of the cluster, and of creating
 * proxy objects (or shards) which can then be used to
 * consistently forward requests to a server.
 *
 * See Readme.md for more details on how to use this class.
 *
 * @export
 * @class AbstractShardedModule
 */
export default abstract class AbstractShardedModule {
  /**
   * Service name
   *
   * By default, the service name will be the name of the class
   *
   * @type {string}
   * @memberof AbstractShardedModule
   */
  public name: string

  /**
   * Hashing algorithm to for sharding
   */
  private hashingAlgorithm: string = 'md5'

  /**
   * Module logger instance
   */
  private logger: mage.core.ILogger

  /**
   * Request event label
   *
   * Request messages follow the following format:
   *
   * [requestId, method, ...args]
   *
   * @type {string}
   * @memberof AbstractShardedModule
   */
  private REQUEST_EVENT_NAME: string

  /**
   * Response event label
   *
   * Response messages follow the following format:
   *
   * [requestId, response, error]
   *
   * @type {string}
   * @memberof AbstractShardedModule
   */
  private RESPONSE_EVENT_NAME: string

  /**
   * Service instance
   *
   * @type {mage.core.IService}
   * @memberof AbstractShardedModule
   */
  private service: mage.core.IService

  /**
   * Hashes of available workers in the clusters
   *
   * A given node in the cluster may be running more than
   * one worker; each worker will have its own accessible address.
   *
   * @type {string[]}
   * @memberof AbstractShardedModule
   */
  private addressHashes: string[] = []

  /**
   * The address hash for this local server
   *
   * This is used for two purposes:
   *
   *   1. Call directly methods locally if the shard points to the
   *      local server
   *
   *   2. Allow for the creation of a shard reference that points to the
   *      local server
   *
   * @type {string[]}
   * @memberof AbstractShardedModule
   */
  private localNodeHash: string

  /**
   * Hash to MMRP address key-value map
   *
   * @type {{ [hash: string]: string[] }}
   * @memberof AbstractShardedModule
   */
  private clusterAddressMap: { [hash: string]: string[] } = {}

  /**
   * Number of nodes in the cluster
   *
   * @type {number}
   * @memberof AbstractShardedModule
   */
  private clusterSize: number = 0

  /**
   * Pendng requests that are being executed on a remote MAGE node
   *
   * When requests are forwarded to a remote MAGE node, a reference to
   * the original request will be placed here; upon reception of
   * the response (an error or a value), the code execution
   * will then continue locally.
   *
   * Requests will timeout after a certain amount of time
   *
   * @type {Map<IShardedRequestMeta, ShardedRequest>}
   * @memberof AbstractShardedModule
   */
  private pendingRequests: Map<IShardedRequestMeta, ShardedRequest> = new Map()

  /**
   * Key-value for fetching the pendingRequests
   * map key
   *
   * This is used to allow quick by-reference access to
   * pending requests stored in this.pendingRequests
   *
   * ```typescript
   * const key = this.pendingRequestsKeyMap['some-id']
   * const request = this.pendingRequests(key)
   * ```
   *
   * @memberof AbstractShardedModule
   */
  private pendingRequestsKeyMap: {
    [id: string]: IShardedRequestMeta
  } = {}

  /**
   * Garbage collection timer reference for dealing with stalled requests
   *
   * Should requests not be replied to within a certain amount of time,
   * an error will be returned instead.
   *
   * @private
   * @type {number}
   * @memberof AbstractShardedModule
   */
  private gcTimer: NodeJS.Timer | null

  /**
   * Creates an instance of AbstractShardedModul
   *
   * @param {string} [name]
   * @memberof AbstractShardedModule
   */
  constructor(name?: string, hashingAlgorithm: string = 'md5', gcTimeoutTime: number = 5 * 1000) {
    if (!name) {
      name = this.getClassName()
    }

    this.name = name
    this.hashingAlgorithm = hashingAlgorithm
    this.REQUEST_EVENT_NAME = `sharded.${name}.request`
    this.RESPONSE_EVENT_NAME = `sharded.${name}.response`

    // Stalled requests garbage collection
    this.scheduleGarbageCollection(gcTimeoutTime)
  }

  /**
   * Setup method called by MAGE during initialization
   *
   * @param {mage.core.IState} _state
   * @param {(error?: Error) => void} callback
   * @memberof AbstractShardedModule
   */
  public async setup(_state: mage.core.IState, callback: (error?: Error) => void) {
    this.logger = mage.logger.context('ShardedModule', this.name)

    const {
      name,
      REQUEST_EVENT_NAME,
      RESPONSE_EVENT_NAME
    } = this

    // See https://github.com/dylang/shortid#shortidworkerinteger
    /* istanbul ignore if */
    if (cluster.isWorker) {
      const id = cluster.worker.id
      shortid.worker(id % 16)
    }

    const mmrpNode = this.getMmrpNode()

    /* istanbul ignore if */
    if (!mmrpNode) {
      return callback(new Error('mmrpNode does not exist. Did you configure mmrp and service discovery in your config file ?'))
    }

    // Cluster communication - run module method locally, and forward the response
    mmrpNode.on(`delivery.${REQUEST_EVENT_NAME}`, async (requestEnvelope) => {
      const request = requestEnvelope.messages
      const requestId = (<any> request.shift()).toString()

      let responseError
      let responseData

      try {
        responseData = await this.onRequest(request)
      } catch (e) {
        responseError = e
      } finally {
        const Envelope = msgServer.mmrp.Envelope
        const response = responseError ? 'false' : JSON.stringify(responseData)
        const error = responseError ? serializeError(responseError) : 'false'
        const messages = [requestId, response, error]
        const responseEnvelope = new Envelope(this.RESPONSE_EVENT_NAME, messages, requestEnvelope.returnRoute)

        mmrpNode.send(responseEnvelope, 1, (mmrpError?: Error) => {
          /* istanbul ignore if */
          if (mmrpError) {
            this.logger.error('Error sending reply', error)
          }
        })
      }
    })

    mmrpNode.on(`delivery.${RESPONSE_EVENT_NAME}`, async (envelope) => this.onResponse(envelope.messages))

    // Service information tracking
    const service = this.service = this.getServiceDiscovery().createService(name, 'tcp')
    const address = [(<any> mmrpNode).clusterId, (<any> mmrpNode).identity]

    service.on('up', (node: mage.core.IServiceNode) => this.registerNodeAddress(node))
    service.on('down', (node: mage.core.IServiceNode) => this.unregisterNodeAddress(node))

    service.announce(this.getPseudoPort(), address, (error?: Error) => {
      /* istanbul ignore if */
      if (error) {
        return callback(error)
      }

      service.discover()

      callback()
    })

    this.localNodeHash = this.hash(address.join(''))
  }

  /**
   * Teardown method called by MAGE during shutdown
   *
   * @param {mage.core.IState} _state
   * @param {(error?: Error) => void} callback
   * @memberof AbstractShardedModule
   */
  /* istanbul ignore next */
  public teardown(_state: mage.core.IState, callback: (error?: Error) => void) {
    if (!this.service) {
      callback()
    }

    (<any> this.service).close(callback)
  }

  /**
   * Retrieve a shard using a deserialized shard instance (IShard)
   *
   * @param {IShard} shard
   */
  public getShard(shard: IShard): Shard<this> {
    const { id } = shard
    const address = this.clusterAddressMap[id]
    const toJson = function () {
      return { id }
    }

    return new Proxy<this>(this, {
      get: (target: AbstractShardedModule, name: string) => {
        // Return shard ID if requested
        if (name === 'toJSON') {
          return toJson
        }

        if (name === 'id') {
          return id
        }

        if (name === 'inspect') {
          return function () {
            return { id }
          }
        }

        // Only functions are made available by the proxy
        const val = (<any> target)[name]

        if (isFunction(val) !== true) {
          // Do not send local requests over the network
          if (id === this.localNodeHash) {
            return Promise.resolve(val)
          }

          this.assertClusterId(id)
          return this.addPendingRequest(address, name).send()
        }

        // Encapsulate request
        return async (...args: any[]) => {
          if (id === this.localNodeHash) {
            return val.bind(this)(...args)
          }

          this.assertClusterId(id)
          return this.addPendingRequest(address, name, args).send()
        }
      },
      has(_target: any, key: string) {
        return key === 'id'
      },
      ownKeys() {
        return ['id']
      }
    }) as any
  }

  /**
   * Retrieve a shard using a deserialized shard instance (IShard)
   *
   *
   */
  public createBroadcast(): Broadcast<this> {
    return new Proxy<this>(this, {
      get: (target: AbstractShardedModule, name: string) => {
        const val = (<any> target)[name]
        const hashes = Object.keys(this.clusterAddressMap)

        const promises: Array<Promise<any>> = []
        const responses: { [key: string]: any } = {}
        const errors: { [key: string]: Error } = {}

        const data = async () => {
          await Promise.all(promises)

          if (Object.keys(errors).length > 0) {
            return [errors, responses]
          } else {
            return [null, responses]
          }
        }

        if (isFunction(val) !== true) {
          for (const id of hashes) {
            const shard = target.getShard({ id }) as any
            const promise = shard[name]
              .then((response: any) => responses[id] = response)
              .catch((error: Error) => errors[id] = error)

            promises.push(promise)
          }

          return data()
        }

        // Encapsulate request
        return async (...args: any[]) => {
          for (const id of hashes) {
            const shard = target.getShard({ id }) as any
            const promise = shard[name](...args)
              .then((response: any) => responses[id] = response)
              .catch((error: Error) => errors[id] = error)

            promises.push(promise)
          }

          return data()
        }
      },
      /* istanbul ignore next */
      has(_target: any, _key: string) {
        return false
      },
      /* istanbul ignore next */
      ownKeys() {
        return []
      }
    }) as any
  }

  /**
   * Retrieve the local shard value
   *
   * Useful when you wish to make it so that future requests
   * be routed to the current local server.
   *
   * Note that this does NOT return a proxy; this returns just the forwardable
   * data of a proxy.
   */
  public getLocalShard() {
    return { id: this.localNodeHash }
  }

  /**
   * Create a new shard from a shard key
   *
   * You will want to call this to receive the shard
   * reference. Then, for subsequent related calls,
   * you will want to make sure to use the returned
   * reference to forward your requests.
   *
   * @param {string} shardKey
   */
  public createShard(shardKey: string) {
    const shardId = this.getShardId(shardKey)
    return this.getShard(shardId)
  }

  /**
   *
   * @param {string} shardKey
   */
  protected getShardId(shardKey: string) {
    const hash: Buffer = (<any> this).hash(shardKey, 'buffer')
    let sum = 0

    // Fastest way I could find to walk through the Buffer
    // See: https://stackoverflow.com/a/3762735/262831
    for (let i = hash.length; i--;) {
      sum += hash[i]
    }

    return {
      id: this.addressHashes[sum % this.clusterSize]
    }
  }

  /**
   * Hash a string using the configured algorithm
   *
   * Inspired by https://github.com/3rd-Eden/node-hashring/blob/master/index.js#L13
   *
   * @param str
   */
  private hash(str: string, encoding: string = 'hex') {
    return crypto.createHash(this.hashingAlgorithm).update(str).digest(<any> encoding)
  }

  /* istanbul ignore next */
  /* tslint:disable-next-line:prefer-function-over-method */
  private getMmrpNode() {
    return msgServer.getMmrpNode()
  }

  /* istanbul ignore next */
  /* tslint:disable-next-line:prefer-function-over-method */
  private getServiceDiscovery() {
    return serviceDiscovery
  }

  /**
   * Make up a fake port
   *
   * The current serviceDiscovery will consider an announced service to
   * be the same if the hostnames and ports are the same; this is an issue in
   * our case, since each workers on a single server will be announced,
   * and one might want to run more than one MAGE instance on a single server.
   *
   * To palliate to this issue, we pretend the PID is a port. Since PID's, depending
   * on the subsystem, can possibly be very large (Linux's default is 32768, but is
   * configurable through /proc/sys/kernel/pid_max), we use a modulo operator to limit
   * how big the port can be.
   *
   * 2017/07 (stelcheck): This is a huge hack, and I am aware this may break under specific
   * circumstances; namely, if other external services are announced on the same host,
   * on the same port. If you believe you are hitting this case, please let me know.
   */
  /* istanbul ignore next */
  /* tslint:disable-next-line:prefer-function-over-method */
  private getPseudoPort() {
    return (process.pid % 65535) + 1
  }

  /**
   * Retrieve the current classes's name
   *
   * @returns {string}
   * @memberof AbstractShardedModule
   */
  private getClassName(): string {
    return this.constructor.name
  }

  /**
   * Schedule garbage collection
   *
   * Note that this will cancel any previously scheduled GC, the timeout
   * time value passed as an argument will be used for all future GC schedulings
   *
   * @private
   * @param {number} gcTimeoutTime
   * @memberof AbstractShardedModule
   */
  private scheduleGarbageCollection(gcTimeoutTime: number) {
    if (this.gcTimer) {
      clearTimeout(this.gcTimer)
    }

    this.gcTimer = setTimeout(() => {
      // Clean up the timer reference
      this.gcTimer = null

      const now = Date.now()
      const keys = this.pendingRequests.keys()

      for (const key of keys) {
        // All other requests are valid, cancel iteration
        if (key.timestamp >= now - gcTimeoutTime) {
          break
        }

        // Reject the request
        const request = this.getAndDeletePendingRequest(key.id)
        request.reject(new Error('Request timed out'))
      }

      // Schedule next GC
      this.scheduleGarbageCollection(gcTimeoutTime)
    }, gcTimeoutTime)
  }

  /**
   *
   *
   * @private
   * @param {mage.core.IServiceNode} node
   * @memberof AbstractShardedModule
   */
  private registerNodeAddress(node: mage.core.IServiceNode) {
    const address: string[] = node.data
    const hash = this.hash(address.join(''))

    if (this.clusterAddressMap[hash]) {
      this.logger.warning.data(node).log('Tried to re-register a known node')
      return
    }

    this.logger.notice.data(node).log('Registering new node')

    // Add adress to our map

    // これではなかろうか
    this.clusterAddressMap[hash] = address

    // Add hash to our list of accessible addresses
    this.addressHashes.push(hash)
    this.addressHashes.sort()

    this.clusterSize += 1
  }

  /**
   *
   *
   * @private
   * @param {mage.core.IServiceNode} node
   * @memberof AbstractShardedModule
   */
  private unregisterNodeAddress(node: mage.core.IServiceNode) {
    const address: string[] = node.data
    const hash = this.hash(address.join(''))

    // Remove hash from list of accessible address
    const index = this.addressHashes.indexOf(hash)

    if (index === -1) {
      this.logger.warning.data(node).log('Tried to unregister an unknown node')
      return
    }

    this.logger.notice.data(node).log('Unegistering node')

    this.addressHashes.splice(index, 1)

    // Remove address from map
    delete this.clusterAddressMap[hash]

    // Reduce cluster size
    this.clusterSize -= 1
  }

  /**
   *
   *
   * @private
   * @param {MmrpEnvelopeMessage[]} messages
   * @returns
   * @memberof AbstractShardedModule
   */
  private async onRequest(messages: MmrpEnvelopeMessage[]) {
    const rawAttributeName = messages.shift()
    const rawArgs = messages.shift()

    if (!rawAttributeName) {
      throw new Error('Method name is missing')
    }

    const attributeName = rawAttributeName.toString()
    if (!rawArgs) {
      return (this as any)[attributeName]
    }

    const method: (...args: any[]) => any = (<any> this)[attributeName]
    const args = JSON.parse(rawArgs.toString())

    if (!method) {
      throw new Error(`Method is not locally available (requested method: ${attributeName})`)
    }

    return method.apply(this, args)
  }

  /**
   * Process a response
   *
   * @private
   * @param {MmrpEnvelopeMessage[]} messages
   * @returns
   * @memberof AbstractShardedModule
   */
  private async onResponse(messages: MmrpEnvelopeMessage[]) {
    const [
      requestId,
      rawData,
      rawError
    ] = messages

    const request = this.getAndDeletePendingRequest(requestId.toString())

    // For void methods (no return, or undefined attributes)
    if (messages.length < 3) {
      return request.resolve(undefined)
    }

    if (rawError && rawError.toString() !== 'false') {
      const data = JSON.parse(rawError.toString())
      const error = new RemoteError(data)

      return request.reject(error)
    }

    request.resolve(JSON.parse(rawData.toString()))
  }

  /**
   * Create a request, and add it to our list of pending requests
   *
   * The request is returned so that the calling code may
   * call the request's `send` method and `await` a response.
   *
   * @private
   * @param {string[]} target
   * @param {string} method
   * @param {any[]} args
   * @returns
   * @memberof AbstractShardedModule
   */
  private addPendingRequest(target: string[], method: string, args?: any[]) {
    const id = shortid.generate()
    const timestamp = Date.now()

    const key: IShardedRequestMeta = { id, timestamp }
    this.pendingRequestsKeyMap[id] = key

    const request = new ShardedRequest(id, this.getMmrpNode(), this.REQUEST_EVENT_NAME, target, method, args)
    this.pendingRequests.set(key, request)

    return request
  }

  /**
   * Retrieve a pending response by request ID
   *
   * @private
   * @param {string} id
   * @returns
   * @memberof AbstractShardedModule
   */
  private getPendingRequest(id: string) {
    const key = this.pendingRequestsKeyMap[id]

    if (!key) {
      throw new Error(`Key not found in request key map (id: ${id})`)
    }

    const request = this.pendingRequests.get(key)

    if (!request) {
      throw new Error(`Pending request not found (id: ${key.id}, timestamp: ${key.timestamp})`)
    }

    return request
  }

  /**
   * Delete a pending response
   *
   * This is normally called once a request has been completed,
   * or when a request has timed out.
   *
   * @private
   * @param {string} id
   * @memberof AbstractShardedModule
   */
  private deletePendingRequest(id: string) {
    const key = this.pendingRequestsKeyMap[id]

    if (!key) {
      throw new Error(`Key not found in request key map (id: ${id})`)
    }

    delete this.pendingRequestsKeyMap[id]
    this.pendingRequests.delete(key)
  }

  /**
   * Retrieve a request, by ID, then delete it from the
   * list of pending requests
   *
   * @private
   * @param {string} id
   * @returns
   * @memberof AbstractShardedModule
   */
  private getAndDeletePendingRequest(id: string) {
    const request = this.getPendingRequest(id)
    this.deletePendingRequest(id)

    return request
  }

  /**
   * Ensure that we know this cluster ID
   *
   * @param id
   */
  private assertClusterId(id: string) {
    if (!this.clusterAddressMap[id]) {
      throw new RemoteError({
        message: 'Remote node is no longer available'
      })
    }
  }
}
