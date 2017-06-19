{ expect, assert }  = require 'chai'
sinon               = require 'sinon'
_                   = require 'lodash'
MongoStorage        = require.main.require 'lib'
LeanRC              = require 'LeanRC'
{
  Utils: { co }
  Query
}                   = LeanRC::
{
  MongoClient
  GridFSBucket
}                   = require 'mongodb'
Parser              = require 'mongo-parse' #mongo-parse@2.0.2
moment              = require 'moment'

createModuleClass = (root = __dirname) ->
  class Test extends LeanRC
    @inheritProtected()
    @include MongoStorage
    @root root
  Test.initialize()

createCollectionClass = (Module) ->
  class Module::MongoCollection extends Module::Collection
    @inheritProtected()
    @include Module::MongoCollectionMixin
    @module Module
  Module::MongoCollection.initialize()

createRecordClass = (Module) ->
  class TestRecord extends Module::Record
    @inheritProtected()
    @module Module
    @attribute cid: Number, default: -1
    @attribute data: String, default: ''
    @public init: Function,
      default: ->
        @super arguments...
        @type = 'Test::TestRecord'
  TestRecord.initialize()

describe 'MongoCollectionMixin', ->
  __db = null
  connectionData =
    username: null
    password: null
    host: 'localhost'
    port: '27017'
    default_db: 'just_for_test'
    db: 'just_for_test'
    collection: 'test_thames_travel'

  # db_url = "mongodb://localhost:27017/just_for_test?authSource=admin"
  credentials = ''
  { username, password, host, port, default_db } = connectionData
  if username and password
    credentials =  "#{username}:#{password}@"
  db_url = "mongodb://#{credentials}#{host}:#{port}/#{default_db}?authSource=admin"

  before ->
    co ->
      # # db_url = "mongodb://localhost:27017/just_for_test?authSource=admin"
      # credentials = ''
      # { username, password, host, port, default_db } = connectionData
      # if username and password
      #   credentials =  "#{username}:#{password}@"
      # db_url = "mongodb://#{credentials}#{host}:#{port}/#{default_db}?authSource=admin"
      __db = yield MongoClient.connect db_url
      dbCollection = yield __db.createCollection 'test_thames_travel'
      date = new Date()
      yield dbCollection.save cid: 1, data: 'three', createdAt: date, updatedAt: date
      date = new Date()
      yield dbCollection.save cid: 2, data: 'men', createdAt: date, updatedAt: date
      date = new Date Date.now() + 1000
      yield dbCollection.save cid: 3, data: 'in', createdAt: date, updatedAt: date
      yield dbCollection.save cid: 4, data: 'a boat', createdAt: date, updatedAt: date
      yield return
  after ->
    co ->
      yield __db.dropCollection 'test_thames_travel'
      __db.close()
      yield return

  describe '.new', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = TestCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, TestCollection
        yield return

  describe '#connection', ->
    it 'Check "connection" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        { db: dbName, collection: nativeCollectionName } = connectionData
        connection = yield collection.connection
        assert.isTrue TestCollection[TestCollection.classVariables['_connection'].pointer]?
        voDB = yield connection.db dbName
        nativeCollection = yield voDB.collection nativeCollectionName
        assert.isTrue nativeCollection?
        assert.isTrue (yield nativeCollection.find())?
        yield return

  describe '#collection', ->
    it 'Check "collection" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        nativeCollection = yield collection.collection
        assert.isTrue collection[TestCollection.instanceVariables['_collection'].pointer]?
        assert.isTrue nativeCollection?
        assert.isTrue (yield nativeCollection.find())?
        db = yield MongoClient.connect db_url
        nativeCollection2 = db.collection connectionData.collection
        assert.deepEqual (yield nativeCollection.find().toArray())[0], (yield nativeCollection2.find().toArray())[0]
        yield return

  describe '#bucket', ->
    it 'Check "bucket" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = TestCollection.new()
        assert.isTrue no
        yield return

  describe '#onRegister', ->
    it 'Check correctness logic of the "onRegister" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        assert.isTrue TestCollection[TestCollection.classVariables['_connection'].pointer]?
        yield return

  describe '#operatorsMap', ->
    it 'Check correctness logic of each function in the "operatorsMap" property', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        { operatorsMap } = collection

        assert.isFunction operatorsMap['$and']
        assert.isFunction operatorsMap['$or']
        assert.isFunction operatorsMap['$not']
        assert.isFunction operatorsMap['$nor']

        assert.isFunction operatorsMap['$where']

        assert.isFunction operatorsMap['$eq']
        assert.isFunction operatorsMap['$ne']
        assert.isFunction operatorsMap['$lt']
        assert.isFunction operatorsMap['$lte']
        assert.isFunction operatorsMap['$gt']
        assert.isFunction operatorsMap['$gte']
        assert.isFunction operatorsMap['$in']
        assert.isFunction operatorsMap['$nin']

        assert.isFunction operatorsMap['$all']
        assert.isFunction operatorsMap['$elemMatch']
        assert.isFunction operatorsMap['$size']

        assert.isFunction operatorsMap['$exists']
        assert.isFunction operatorsMap['$type']

        assert.isFunction operatorsMap['$mod']
        assert.isFunction operatorsMap['$regex']

        assert.isFunction operatorsMap['$td']
        assert.isFunction operatorsMap['$ld']
        assert.isFunction operatorsMap['$tw']
        assert.isFunction operatorsMap['$lw']
        assert.isFunction operatorsMap['$tm']
        assert.isFunction operatorsMap['$lm']
        assert.isFunction operatorsMap['$ty']
        assert.isFunction operatorsMap['$ly']

        logicalOperator = operatorsMap['$and'] ['a', 'b', 'c']
        assert.deepEqual logicalOperator, $and: ['a', 'b', 'c']
        logicalOperator = operatorsMap['$or'] ['a', 'b', 'c']
        assert.deepEqual logicalOperator, $or: ['a', 'b', 'c']
        logicalOperator = operatorsMap['$not'] ['a', 'b', 'c']
        assert.deepEqual logicalOperator, $not: ['a', 'b', 'c']
        logicalOperator = operatorsMap['$nor'] ['a', 'b', 'c']
        assert.deepEqual logicalOperator, $nor: ['a', 'b', 'c']

        compOperator = operatorsMap['$eq'] 'a', 3
        assert.deepEqual compOperator, a: $eq: 3
        compOperator = operatorsMap['$eq'] '@a', '@doc.b'
        assert.deepEqual compOperator, a: $eq: 'b'
        compOperator = operatorsMap['$ne'] 'a', 3
        assert.deepEqual compOperator, a: $ne: 3
        compOperator = operatorsMap['$ne'] '@a', '@doc.b'
        assert.deepEqual compOperator, a: $ne: 'b'
        compOperator = operatorsMap['$lt'] 'a', 3
        assert.deepEqual compOperator, a: $lt: 3
        compOperator = operatorsMap['$lt'] '@a', '@doc.b'
        assert.deepEqual compOperator, a: $lt: 'b'
        compOperator = operatorsMap['$lte'] 'a', 3
        assert.deepEqual compOperator, a: $lte: 3
        compOperator = operatorsMap['$lte'] '@a', '@doc.b'
        assert.deepEqual compOperator, a: $lte: 'b'
        compOperator = operatorsMap['$gt'] 'a', 3
        assert.deepEqual compOperator, a: $gt: 3
        compOperator = operatorsMap['$gt'] '@a', '@doc.b'
        assert.deepEqual compOperator, a: $gt: 'b'
        compOperator = operatorsMap['$gte'] 'a', 3
        assert.deepEqual compOperator, a: $gte: 3
        compOperator = operatorsMap['$gte'] '@a', '@doc.b'
        assert.deepEqual compOperator, a: $gte: 'b'
        compOperator = operatorsMap['$in'] 'a', ['b', 'c']
        assert.deepEqual compOperator, a: $in: ['b', 'c']
        compOperator = operatorsMap['$in'] '@a', ['b', 'c']
        assert.deepEqual compOperator, a: $in: ['b', 'c']
        compOperator = operatorsMap['$nin'] 'a', ['b', 'c']
        assert.deepEqual compOperator, a: $nin: ['b', 'c']
        compOperator = operatorsMap['$nin'] '@a', ['b', 'c']
        assert.deepEqual compOperator, a: $nin: ['b', 'c']

        queryOperator = operatorsMap['$all'] '@a', ['b', 'c', 'd']
        assert.deepEqual queryOperator, a: $all: ['b', 'c', 'd']
        queryOperator = operatorsMap['$elemMatch'] '@a', { $gte: 80, $lt: 85 }
        assert.deepEqual queryOperator, a: $elemMatch: { $gte: 80, $lt: 85 }
        queryOperator = operatorsMap['$size'] '@a', 1
        assert.deepEqual queryOperator, a: $size: 1

        queryOperator = operatorsMap['$exists'] 'a', yes
        assert.deepEqual queryOperator, a: $exists: yes
        queryOperator = operatorsMap['$exists'] '@a', yes
        assert.deepEqual queryOperator, a: $exists: yes
        queryOperator = operatorsMap['$type'] 'a', 1
        assert.deepEqual queryOperator, a: $type: 1
        queryOperator = operatorsMap['$type'] '@a', 'string'
        assert.deepEqual queryOperator, a: $type: 'string'

        queryOperator = operatorsMap['$mod'] 'a', [4, 0]
        assert.deepEqual queryOperator, a: $mod: [4, 0]
        queryOperator = operatorsMap['$mod'] '@a', [4, 0]
        assert.deepEqual queryOperator, a: $mod: [4, 0]
        queryOperator = operatorsMap['$regex'] '@a', '/^beep/i'
        assert.deepEqual queryOperator, a: $regex:/^beep/i
        queryOperator = operatorsMap['$regex'] '@a', '/^beep/', 'imxs'
        assert.deepEqual queryOperator, a: {$regex:/^beep/, $options: 'imxs'}
        assert.throws (-> operatorsMap['$where']()) , Error
        assert.throws (-> operatorsMap['$text']()) , Error

        todayStart = moment().startOf('day').toISOString()
        todayEnd = moment().endOf('day').toISOString()
        queryOperator = operatorsMap['$td'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: todayStart
          createdAt: $lt: todayEnd
        ]
        queryOperator = operatorsMap['$td'] '@createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: todayStart
          createdAt: $lt: todayEnd
        ]

        yesterdayStart = moment().subtract(1, 'days').startOf('day').toISOString()
        yesterdayEnd = moment().subtract(1, 'days').endOf('day').toISOString()
        queryOperator = operatorsMap['$ld'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: yesterdayStart
          createdAt: $lt: yesterdayEnd
        ]
        queryOperator = operatorsMap['$ld'] '@createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: yesterdayStart
          createdAt: $lt: yesterdayEnd
        ]

        weekStart = moment().startOf('week').toISOString()
        weekEnd = moment().endOf('week').toISOString()
        queryOperator = operatorsMap['$tw'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: weekStart
          createdAt: $lt: weekEnd
        ]
        queryOperator = operatorsMap['$tw'] '@createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: weekStart
          createdAt: $lt: weekEnd
        ]

        weekStart = moment().subtract(1, 'weeks').startOf('week').toISOString()
        weekEnd = moment().subtract(1, 'weeks').endOf('week').toISOString()
        queryOperator = operatorsMap['$lw'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: weekStart
          createdAt: $lt: weekEnd
        ]
        queryOperator = operatorsMap['$lw'] 'createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: weekStart
          createdAt: $lt: weekEnd
        ]

        monthStart = moment().startOf('month').toISOString()
        monthEnd = moment().endOf('month').toISOString()
        queryOperator = operatorsMap['$tm'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: monthStart
          createdAt: $lt: monthEnd
        ]
        queryOperator = operatorsMap['$tm'] 'createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: monthStart
          createdAt: $lt: monthEnd
        ]

        monthStart = moment().subtract(1, 'months').startOf('month').toISOString()
        monthEnd = moment().subtract(1, 'months').endOf('month').toISOString()
        queryOperator = operatorsMap['$lm'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: monthStart
          createdAt: $lt: monthEnd
        ]
        queryOperator = operatorsMap['$lm'] 'createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: monthStart
          createdAt: $lt: monthEnd
        ]

        yearStart = moment().startOf('year').toISOString()
        yearEnd = moment().endOf('year').toISOString()
        queryOperator = operatorsMap['$ty'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: yearStart
          createdAt: $lt: yearEnd
        ]
        queryOperator = operatorsMap['$ty'] 'createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: yearStart
          createdAt: $lt: yearEnd
        ]

        yearStart = moment().subtract(1, 'years').startOf('year').toISOString()
        yearEnd = moment().subtract(1, 'years').endOf('year').toISOString()
        queryOperator = operatorsMap['$ly'] 'createdAt', yes
        assert.deepEqual queryOperator, $and: [
          createdAt: $gte: yearStart
          createdAt: $lt: yearEnd
        ]
        queryOperator = operatorsMap['$ly'] 'createdAt', no
        assert.deepEqual queryOperator, $not: $and: [
          createdAt: $gte: yearStart
          createdAt: $lt: yearEnd
        ]
        yield return

  # @TODO Нужно будет понипихать сюда больше проверок. Желательно реальных примеров query, и желательно сложных.
  describe '#parseFilter', ->
    it 'Use method "parseFilter" for simple parsed query', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        result = collection.parseFilter
          field: 'a'
          operator: '$eq'
          operand: 'b'
        assert.deepEqual result, a: $eq: 'b'
        yield return
    it 'Use method "parseFilter" for parsed query', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        result = collection.parseFilter
          parts: [
            operator: '$or'
            parts: [
              field: 'c'
              operand: 1
              operator: '$eq'
            ,
              field: '@b'
              operand: 2
              operator: '$eq'
            ]
          ,
            operator: '$nor'
            parts: [
              field: '@d'
              operand: '1'
              operator: '$eq'
            ,
              field: 'b'
              operand: '2'
              operator: '$eq'
            ]
          ]
          operator: '$and'
        assert.deepEqual result, $and: [
          $or: [
            c: $eq: 1
          ,
            b: $eq: 2
          ]
        ,
          $nor: [
            d: $eq: '1'
          ,
            b: $eq: '2'
          ]
        ]
        yield return
    it 'Use method "parseFilter" for parsed query with operator "$elemMatch" and implicitField:no', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        result = collection.parseFilter
          operator: '$elemMatch'
          field: '@a'
          parts: [
            field: '@doc.b'
            operand: 'c'
            operator: '$eq'
          ,
            field: '@doc.d'
            operand: 2
            operator: '$eq'
          ]
          implicitField: no
        assert.deepEqual result, a: $elemMatch:
          b: $eq: 'c'
          d: $eq: 2
        yield return
    it 'Use method "parseFilter" for parsed query with operator "$elemMatch" and implicitField:yes', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        result = collection.parseFilter
          operator: '$elemMatch'
          field: '@a'
          parts: [
            operand: 10
            operator: '$gte'
          ,
            operand: 15
            operator: '$lt'
          ]
          implicitField: yes
        assert.deepEqual result, a: $elemMatch:
          $gte: 10,
          $lt: 15
        yield return

  describe '#parseQuery', ->
    it 'Check correctness logic of method "parseQuery" for "insert" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        testRecord = TestRecord.new { cid: 5, data: '!', createdAt: date, updatedAt: date }, collection
        query = Test::Query.new()
          .insert testRecord
          .into collection.collectionFullName()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$insert': testRecord
          '$into': collection.collectionFullName()
        correctResult =
          queryType: 'insert'
          snapshot:
            cid: 5
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!'
        # assert.deepEqual result1, correctResult
        # assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        res = yield nativeCollection.insertOne correctResult.snapshot,
          w: "majority"
          j: yes
          wtimeout: 500
        assert.strictEqual res.insertedCount, 1
        res = yield nativeCollection.findOne cid: 5
        assert.strictEqual res.data, '!'
        yield return
    it 'Check correctness logic of method "parseQuery" for "update" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        testRecord = TestRecord.new { cid: 5, data: '!!', createdAt: date, updatedAt: date }, collection
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $eq: 5
          .update testRecord
          .into collection.collectionFullName()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $eq: 5
          '$update': testRecord
          '$into': collection.collectionFullName()
        correctResult =
          queryType: 'update'
          filter: $and: [cid: $eq: 5]
          snapshot:
            cid: 5
            id: null
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!!'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        res = yield nativeCollection.updateMany correctResult.filter, $set: correctResult.snapshot,
          multi: yes
          w: "majority"
          j: yes
          wtimeout: 500
        assert.strictEqual res.matchedCount, 1
        assert.strictEqual res.modifiedCount, 1
        res = yield nativeCollection.findOne correctResult.filter
        assert.strictEqual res.data, '!!'
        yield return
    it 'Check correctness logic of method "parseQuery" for "replace" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        testRecord = TestRecord.new { cid: 5, data: '!!!', createdAt: date, updatedAt: date }, collection
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $eq: 5
          .replace testRecord
          .into collection.collectionFullName()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $eq: 5
          '$replace': testRecord
          '$into': collection.collectionFullName()
        correctResult =
          queryType: 'replace'
          filter: $and: [cid: $eq: 5]
          snapshot:
            cid: 5
            id: null
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!!!'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        res = yield nativeCollection.updateMany correctResult.filter, $set: correctResult.snapshot,
          multi: yes
          w: "majority"
          j: yes
          wtimeout: 500
        assert.strictEqual res.matchedCount, 1
        assert.strictEqual res.modifiedCount, 1
        res = yield nativeCollection.findOne correctResult.filter
        assert.strictEqual res.data, '!!!'
        yield return
    it 'Check correctness logic of method "parseQuery" for "remove" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $eq: 5
          .remove()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $eq: 5
          '$remove': yes
        correctResult =
          queryType: 'remove'
          filter: $and: [cid: $eq: 5]
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        res = yield nativeCollection.deleteMany correctResult.filter, $set: correctResult.snapshot,
          w: "majority"
          j: yes
          wtimeout: 500
        assert.strictEqual res.result.ok, 1
        assert.strictEqual res.deletedCount, 1
        res = yield nativeCollection.findOne correctResult.filter
        assert.isFalse res?
        yield return
    it 'Check correctness logic of method "parseQuery" for "count" records', -> # Need mongo version >= 3.4.0
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .count()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$count': yes
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $count: 'result'
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.deepEqual res, result: 2
        yield return
    it 'Check correctness logic of method "parseQuery" for "sum" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .sum 'cid'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$sum': 'cid'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $group:
              _id: null
              result: $sum: '$cid'
          ,
            $project: _id: 0
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.deepEqual res, result: 7 # 3 + 4
        yield return
    it 'Check correctness logic of method "parseQuery" for "min" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .min 'cid'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$min': 'cid'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $sort: cid: 1
          ,
            $limit: 1
          ,
            $project:
              _id: 0
              result: "$cid"
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.deepEqual res, result: 3
        yield return
    it 'Check correctness logic of method "parseQuery" for "max" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .max 'cid'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$max': 'cid'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $sort: cid: -1
          ,
            $limit: 1
          ,
            $project:
              _id: 0
              result: "$cid"
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.deepEqual res, result: 4
        yield return
    it 'Check correctness logic of method "parseQuery" for "avg" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .avg 'cid'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$avg': 'cid'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $group:
              _id: null
              result: $avg: '$cid'
          ,
            $project:
              _id: 0
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.deepEqual res, result: 3.5
        yield return
    it 'Check correctness logic of method "parseQuery" for "sort" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .sort cid: 'DESC'
          .sort data: 'ASC'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$sort': [
            cid: 'DESC'
          ,
            data: 'ASC'
          ]
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $sort:
              cid: -1
              data: 1
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.strictEqual res.cid, 4
        yield return
    it 'Check correctness logic of method "parseQuery" for "limit" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .limit 1

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$limit': 1
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $limit: 1
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 1
        yield return
    it 'Check correctness logic of method "parseQuery" for "offset" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .offset 1

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$offset': 1
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $skip: 1
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.strictEqual res.cid, 4
        yield return
    it 'Check correctness logic of method "parseQuery" for "collect" records, with using "into"', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 1
          .collect date: '$createdAt'
          .into 'data'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 1
          '$collect': date: '$createdAt'
          '$into': 'data'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 1]
          ,
            $group:
              _id: date: '$createdAt'
              data: $push:
                id: '$id'
                rev: '$rev'
                type: '$type'
                cid: '$cid'
                data: '$data'
                isHidden: '$isHidden'
                createdAt: '$createdAt'
                deletedAt: '$deletedAt'
                updatedAt: '$updatedAt'
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 2
        yield return
    it 'Check correctness logic of method "parseQuery" for "collect" records, without using "into"', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 1
          .collect date: '$createdAt'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 1
          '$collect': date: '$createdAt'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 1]
          ,
            $group:
              _id: date: '$createdAt'
              GROUP: $push:
                id: '$id'
                rev: '$rev'
                type: '$type'
                cid: '$cid'
                data: '$data'
                isHidden: '$isHidden'
                createdAt: '$createdAt'
                deletedAt: '$deletedAt'
                updatedAt: '$updatedAt'
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 2
        yield return
    it 'Check correctness logic of method "parseQuery" for "having" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 1
          .having '@doc.cid': $lt: 3

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 1
          '$having': '@doc.cid': $lt: 3
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 1]
          ,
            $match: $and: [cid: $lt: 3]
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.strictEqual res.cid, 2
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 1
        yield return
    it 'Check correctness logic of method "parseQuery" for simple format "return" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .return 'data'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$return': '@doc.data'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $project:
              _id: 0
              data: 1
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.strictEqual res.data, 'in'
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 2
        yield return
    it 'Check correctness logic of method "parseQuery" for complex format "return" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .return superdata: 'data'

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$return': superdata: '@doc.data'
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $addFields:
              superdata: '$data'
          ,
            $project:
              superdata: 1
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.strictEqual res.superdata, 'in'
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 2
        yield return
    it 'Check correctness logic of method "parseQuery" for "return" with "distinct" format records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc.cid': $gt: 2
          .return 'data'
          .distinct()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc.cid': $gt: 2
          '$return': '@doc.data'
          '$distinct': yes
        correctResult =
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $project:
              _id: 0
              data: 1
          ,
            $group: _id: "$$CURRENT"
          ]
          queryType: 'find'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        nativeCollection = yield collection.collection
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.next()
        assert.strictEqual res._id.data, 'a boat'
        cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        res = yield cursor.toArray()
        assert.strictEqual res.length, 2
        yield return
  ###

  describe '#executeQuery', ->
    it 'should send query to ArangoDB', ->
      co ->
        class Test extends LeanRC
          @inheritProtected()
          @include ArangoExtension
          @root __dirname
        Test.initialize()
        class ArangoCollection extends Test::Collection
          @inheritProtected()
          @include Test::QueryableMixin
          @include Test::ArangoCollectionMixin
          @module Test
        ArangoCollection.initialize()
        class SampleRecord extends Test::Record
          @inheritProtected()
          @module Test
          @attribute data: String
          @public init: Function,
            default: ->
              @super arguments...
              @type = 'Test::SampleRecord'
        SampleRecord.initialize()
        collection = ArangoCollection.new 'TEST_COLLECTION',
          delegate: SampleRecord
          serializer: Test::Serializer
        samples = yield collection.executeQuery '
          FOR doc IN test_samples
          SORT doc._key
          RETURN doc
        '
        items = yield samples.toArray()
        assert.lengthOf items, 4
        for item in items
          assert.instanceOf item, SampleRecord
        items = yield collection.executeQuery '
          FOR doc IN test_samples
          FILTER doc.data == "a boat"
          RETURN doc
        '
        item = yield items.first()
        assert.equal item.data, 'a boat'
        yield return

  ####
  ###

  describe '#push', ->
    it 'Check correctness logic of the "push" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#remove', ->
    it 'Check correctness logic of the "hasNext" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#take', ->
    it 'Check correctness logic of the "hasNext" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#takeMany', ->
    it 'Check correctness logic of the "hasNext" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#takeAll', ->
    it 'Check correctness logic of the "hasNext" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        yield return

  describe '#override', ->
  describe '#patch', ->
  describe '#includes', ->
  describe '#length', ->
  describe '#operatorsMap', ->
  describe '#parseFilter', ->
  describe '#parseQuery', ->
  describe '#executeQuery', ->
  describe '#createFileWriteStream', ->
  describe '#createFileReadStream', ->
  describe '#fileExists', ->

  ###
