import AbstractShardedModule from '../src'

class FakeModule extends AbstractShardedModule {
  public test(key: string) {
    return this.getShardId(key).id
  }
}

function printReport(nodeCount: number, result: any, time: number) {
  process.stdout.write(`[${nodeCount} nodes (${time} msec)]\r\n`)

  for (let i = 1; i <= nodeCount; i += 1) {
    const count = result[i]
    const percentage = count / 10000
    process.stdout.write(`\t| ${i}: ${count} (${percentage}%)\t|\r\n`)
  }

  process.stdout.write(`\r\n`)
}

function test(nodeCount: number) {
  const addressHashes = (new Array(nodeCount)).fill(0).map((_val, pos) => pos + 1)

  const instance: FakeModule = Object.assign(new FakeModule(), {
    clusterSize: nodeCount,
    addressHashes
  })

  const result: any = addressHashes.reduce((container, val) => Object.assign(container, { [val]: 0}), {})

  const start = Date.now()
  for (let i = 0; i < 1000; i += 1) {
    for (let j = 0; j < 1000; j += 1) {
      const instanceHash = instance.test(`some/${i}/key/${j}`)
      result[instanceHash] += 1
    }
  }
  const end = Date.now()

  printReport(nodeCount, result, end - start)
}

for (let i = 3; i <= 20; i += 1) {
  test(i)
}

process.exit(0)
