import * as cluster from 'cluster'
import * as mage from 'mage'

const {
  msgServer,
  serviceDiscovery
} = mage.core

const isFunction = require('is-function-x')
const md5 = require('md5')
const errorToObject = require('serialize-error')
const shortid = require('shortid')

/**
 * This type is not exposed at the moment by MAGE, so
 * we imported its definition back into this project.
 */
type MmrpEnvelopeMessage = string | Buffer

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
  public args: any[]

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

  constructor(id: string, mmrpNode: any, eventName: string, target: string[], method: string, args: any[]) {
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

function getAddressHash(address: string[]): string {
  return md5(address.join(''))
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
  constructor(name?: string, gcTimeoutTime: number = 5 * 1000) {
    if (!name) {
      name = this.getClassName()
    }

    this.name = name
    this.logger = mage.logger.context('ShardedModule', name)
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
    const {
      name,
      REQUEST_EVENT_NAME,
      RESPONSE_EVENT_NAME
    } = this

    // See https://github.com/dylang/shortid#shortidworkerinteger
    /* istanbul ignore if */
    if (cluster.isWorker) {
      const id = parseInt(cluster.worker.id, 10)
      shortid.worker(id % 16)
    }

    const mmrpNode = this.getMmrpNode()

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
        const response = responseData ? JSON.stringify(responseData) : 'false'
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

    service.on('up', (node: mage.core.IServiceNode) => this.registerNodeAddress(node))
    service.on('down', (node: mage.core.IServiceNode) => this.unregisterNodeAddress(node))
    service.discover()

    service.announce(8080, [(<any> mmrpNode).clusterId, (<any> mmrpNode).identity], callback)
  }

  /**
   *
   * @param {IShard} shard
   */
  public getShard(shard: IShard) {
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

        // Only functions are made available by the proxy
        const val = (<any> target)[name]

        if (isFunction(val) !== true) {
          return val // Todo: add get/set request system?
        }

        // Encapsulate request
        return async (...args: any[]) => {
          if (!this.clusterAddressMap[id]) {
            throw new RemoteError({
              message: 'Remote node is no longer available'
            })
          }

          const request = this.addPendingRequest(address, name, args)

          return request.send()
        }
      },
      has(_target: any, key: string) {
        return key === 'id'
      },
      ownKeys() {
        return ['id']
      }
    })
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
  private getShardId(shardKey: string) {
    const md5sum = md5(shardKey)
    const pos = parseInt(md5sum, 16) % this.clusterSize

    return {
      id: this.addressHashes[pos]
    }
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
    const hash = getAddressHash(address)

    if (this.clusterAddressMap[hash]) {
      this.logger.warning.data(node).log('Tried to re-register a known node')
      return
    }

    this.logger.notice.data(node).log('Registering new node')

    // Add adress to our map
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
    const hash = getAddressHash(address)

    // Remove hash from list of accessible address
    const index = this.addressHashes.indexOf(hash)

    if (index === -1) {
      this.logger.warning.data(node).log('Tried to unregister an unknown node')
      return
    }

    this.logger.notice.data(node).log('Unegistering new node')

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
    const rawMethodName = messages.shift()
    const rawArgs = messages.shift()

    if (!rawMethodName) {
      throw new Error('Method name is missing')
    }

    if (!rawArgs) {
      throw new Error('Arguments are missing')
    }

    const methodName = rawMethodName.toString()
    const method: (...args: any[]) => any = (<any> this)[methodName]
    const args = JSON.parse(rawArgs.toString())

    if (!method) {
      throw new Error(`Method is not locally available (requested method: ${methodName})`)
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
  private addPendingRequest(target: string[], method: string, args: any[]) {
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
}
