fs                  = require 'fs'
crypto              = require 'crypto'
{ expect, assert }  = require 'chai'
sinon               = require 'sinon'
_                   = require 'lodash'
MongoStorage        = require.main.require 'lib'
LeanRC              = require '@leansdk/leanrc/lib'
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

configs = null

createModuleClass = (root = __dirname, name = 'Test') ->
  TestModule = class extends LeanRC
    @inheritProtected()
    @root root
    @include MongoStorage
    @initialize()
  Reflect.defineProperty TestModule, 'name', value: name
  TestModule

createCollectionClass = (Module, name = 'MongoCollection') ->
  TestCollection = class extends Module::Collection
    @inheritProtected()
    @include Module::QueryableCollectionMixin
    @include Module::MongoCollectionMixin
    @module Module
    @initialize()
    @public configs: Object, { default: configs }
  Reflect.defineProperty TestCollection, 'name', value: name
  TestCollection

createRecordClass = (Module, name = 'TestRecord') ->
  TestRecord = class extends Module::Record
    @inheritProtected()
    @module Module
    @attribute cid: Number,
      default: -1
    @attribute data: String,
      default: ''
    @initialize()
  Reflect.defineProperty TestRecord, 'name', value: name
  TestRecord

connections = {}


describe 'MongoCollectionMixin', ->
  __db = null
  connectionData =
    mongodb:
      username: null
      password: null
      host: 'localhost'
      port: '27017'
      dbName: 'just_for_test'
    default_db: 'just_for_test'
    dbGridFS: 'just_for_test_gridfs'
    collection: 'test_tests'#'test_thames_travel'
  configs = mongodb: connectionData.mongodb

  # db_url = "mongodb://localhost:27017/just_for_test?authSource=admin"
  { mongodb: {username, password, host, port}, default_db } = connectionData
  credentials = if username and password then "#{username}:#{password}@" else ''
  db_url = "mongodb://#{credentials}#{host}:#{port}/#{default_db}?authSource=admin"

  createConnection = (dbName) ->
    co ->
      unless connections[dbName]?
        { username, password, host, port } = connectionData.mongodb
        creds = if username and password then "#{username}:#{password}@" else ''
        dbUrl = "mongodb://#{creds}#{host}:#{port}/#{dbName}?authSource=admin"
        connections[dbName] = yield MongoClient.connect dbUrl
      connections[dbName]

  before ->
    co ->
      { default_db } = connectionData
      __db = yield createConnection default_db
      dbCollection = yield __db.createCollection 'test_tests'#'test_thames_travel'
      date = new Date().toISOString()
      yield dbCollection.save id: 'q1', type: 'Test::TestRecord', cid: 1, data: 'three', createdAt: date, updatedAt: date
      date = new Date().toISOString()
      yield dbCollection.save id: 'w2', type: 'Test::TestRecord', cid: 2, data: 'men', createdAt: date, updatedAt: date
      date = new Date(Date.now() + 1000).toISOString()
      yield dbCollection.save id: 'e3', type: 'Test::TestRecord', cid: 3, data: 'in', createdAt: date, updatedAt: date
      yield dbCollection.save id: 'r4', type: 'Test::TestRecord', cid: 4, data: 'a boat', createdAt: date, updatedAt: date
      yield return
  after ->
    co ->
      for connectionName, connection of connections
        yield connection.dropDatabase()
        yield connection.close yes
      yield return

  describe '.new', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        # collection.onRegister()
        assert.isTrue collection?
        assert.instanceOf collection, TestCollection
        # yield collection.onRemove()
        yield return

  describe '#connection', ->
    it 'Check "connection" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        { mongodb: {dbName}, collection: nativeCollectionName } = connectionData
        connection = yield collection.connection
        # assert.isTrue TestCollection[TestCollection.classVariables['_connection'].pointer]?
        assert.isTrue connection?
        voDB = yield createConnection dbName
        nativeCollection = yield voDB.collection nativeCollectionName
        assert.isTrue nativeCollection?
        assert.isTrue (yield nativeCollection.find())?
        yield collection.onRemove()
        yield return

  describe '#collection', ->
    it 'Check "collection" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        nativeCollection = yield collection.collection
        assert.isTrue collection[TestCollection.instanceVariables['_collection'].pointer]?
        assert.isTrue nativeCollection?
        assert.isTrue (yield nativeCollection.find())?
        db = yield createConnection default_db
        nativeCollection2 = db.collection connectionData.collection
        assert.deepEqual (yield nativeCollection.find().toArray())[0], (yield nativeCollection2.find().toArray())[0]
        yield collection.onRemove()
        yield return

  describe '#bucket', ->
    it 'Check "bucket" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        bucket = yield collection.bucket
        assert.isTrue collection[TestCollection.instanceVariables['_bucket'].pointer]?
        yield collection.onRemove()
        yield return

  describe '#onRegister', ->
    it 'Check correctness logic of the "onRegister" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        # assert.isTrue TestCollection[TestCollection.classVariables['_connection'].pointer]?
        assert.isTrue yes # TODO: Find out correct testing way
        yield collection.onRemove()
        yield return

  describe '#operatorsMap', ->
    it 'Check correctness logic of each function in the "operatorsMap" property', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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

        todayStart = moment().utc().startOf('day').toISOString()
        todayEnd = moment().utc().endOf('day').toISOString()
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

        yesterdayStart = moment().utc().subtract(1, 'days').startOf('day').toISOString()
        yesterdayEnd = moment().utc().subtract(1, 'days').endOf('day').toISOString()
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

        weekStart = moment().utc().startOf('week').toISOString()
        weekEnd = moment().utc().endOf('week').toISOString()
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

        weekStart = moment().utc().subtract(1, 'weeks').startOf('week').toISOString()
        weekEnd = moment().utc().subtract(1, 'weeks').endOf('week').toISOString()
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

        monthStart = moment().utc().startOf('month').toISOString()
        monthEnd = moment().utc().endOf('month').toISOString()
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

        monthStart = moment().utc().subtract(1, 'months').startOf('month').toISOString()
        monthEnd = moment().utc().subtract(1, 'months').endOf('month').toISOString()
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

        yearStart = moment().utc().startOf('year').toISOString()
        yearEnd = moment().utc().endOf('year').toISOString()
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

        yearStart = moment().utc().subtract(1, 'years').startOf('year').toISOString()
        yearEnd = moment().utc().subtract(1, 'years').endOf('year').toISOString()
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
        yield collection.onRemove()
        yield return

  # @TODO Нужно будет понипихать сюда больше проверок. Желательно реальных примеров query, и желательно сложных.
  describe '#parseFilter', ->
    it 'Use the "parseFilter" method for simple parsed query', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        result = collection.parseFilter
          field: 'a'
          operator: '$eq'
          operand: 'b'
        assert.deepEqual result, a: $eq: 'b'
        yield collection.onRemove()
        yield return
    it 'Use the "parseFilter" method for parsed query', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
        yield collection.onRemove()
        yield return
    it 'Use the "parseFilter" method for parsed query with operator "$elemMatch" and implicitField:no', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
        yield collection.onRemove()
        yield return
    it 'Use the "parseFilter" method for parsed query with operator "$elemMatch" and implicitField:yes', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
        yield collection.onRemove()
        yield return

  describe '#parseQuery', ->
    ###
    it 'Check correctness logic of the "parseQuery" method for "insert" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
            id: null
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!'
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # res = yield nativeCollection.insertOne correctResult.snapshot,
        #   w: "majority"
        #   j: yes
        #   wtimeout: 500
        # assert.strictEqual res.insertedCount, 1
        # res = yield nativeCollection.findOne cid: 5
        # assert.strictEqual res.data, '!'
        yield return
        yield collection.onRemove()
    it 'Check correctness logic of the "parseQuery" method for "update" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # res = yield nativeCollection.updateMany correctResult.filter, $set: correctResult.snapshot,
        #   multi: yes
        #   w: "majority"
        #   j: yes
        #   wtimeout: 500
        # assert.strictEqual res.matchedCount, 1
        # assert.strictEqual res.modifiedCount, 1
        # res = yield nativeCollection.findOne correctResult.filter
        # assert.strictEqual res.data, '!!'
        yield return
        yield collection.onRemove()
    it 'Check correctness logic of the "parseQuery" method for "replace" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # res = yield nativeCollection.updateMany correctResult.filter, $set: correctResult.snapshot,
        #   multi: yes
        #   w: "majority"
        #   j: yes
        #   wtimeout: 500
        # assert.strictEqual res.matchedCount, 1
        # assert.strictEqual res.modifiedCount, 1
        # res = yield nativeCollection.findOne correctResult.filter
        # assert.strictEqual res.data, '!!!'
        yield return
        yield collection.onRemove()
    ###
    it 'Check correctness logic of the "parseQuery" method for "remove" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        date = new Date()
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .into collection.collectionFullName()
          .filter '@doc.cid': $eq: 5
          .remove('@doc')
        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$into': collection.collectionFullName()
          '$filter': '@doc.cid': $eq: 5
          '$remove': '@doc'
        correctResult =
          queryType: 'removeBy'
          pipeline: [ "$match": "$and": ["cid": "$eq": 5] ]
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # res = yield nativeCollection.deleteMany correctResult.filter, $set: correctResult.snapshot,
        #   w: "majority"
        #   j: yes
        #   wtimeout: 500
        # assert.strictEqual res.result.ok, 1
        # assert.strictEqual res.deletedCount, 1
        # res = yield nativeCollection.findOne correctResult.filter
        # assert.isFalse res?
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "count" records', -> # Need mongo version >= 3.4.0
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.deepEqual res, result: 2
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "sum" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.deepEqual res, result: 7 # 3 + 4
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "min" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.deepEqual res, result: 3
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "max" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.deepEqual res, result: 4
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "avg" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.deepEqual res, result: 3.5
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "sort" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.strictEqual res.cid, 4
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "limit" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 1
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "offset" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.strictEqual res.cid, 4
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "collect" records, with using "into"', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 2
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "collect" records, without using "into"', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 2
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "having" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: no
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.strictEqual res.cid, 2
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 1
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for simple format "return" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.strictEqual res.data, 'in'
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 2
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for complex format "return" records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.strictEqual res.superdata, 'in'
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 2
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "return" with "distinct" format records', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
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
          queryType: 'query'
          isCustomReturn: yes
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult

        # nativeCollection = yield collection.collection
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.next()
        # assert.strictEqual res._id.data, 'a boat'
        # cursor = yield nativeCollection.aggregate correctResult.pipeline, cursor: batchSize: 1
        # res = yield cursor.toArray()
        # assert.strictEqual res.length, 2
        yield collection.onRemove()
        yield return

  describe '#executeQuery', ->
    ###
    it 'Check correctness logic of the "executeQuery" method for "insert" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        date = new Date()
        query =
          queryType: 'insert'
          snapshot:
            cid: 6
            id: null
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '?'
        result = yield collection.executeQuery query
        resultArray = yield result.toArray()
        assert.instanceOf resultArray[0], TestRecord
        assert.strictEqual resultArray[0].cid, query.snapshot.cid
        assert.strictEqual resultArray[0].data, query.snapshot.data
        yield return
        yield collection.onRemove()
    it 'Check correctness logic of the "executeQuery" method for "update" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        date = new Date()
        query =
          queryType: 'update'
          filter: $and: [cid: $eq: 6]
          snapshot:
            cid: 6
            id: null
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '??'
        result = yield collection.executeQuery query
        resultArray = yield result.toArray()
        assert.strictEqual resultArray[0].cid, query.snapshot.cid
        assert.strictEqual resultArray[0].data, query.snapshot.data
        yield return
        yield collection.onRemove()
    it 'Check correctness logic of the "executeQuery" method for "replace" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        date = new Date()
        query =
          queryType: 'replace'
          filter: $and: [cid: $eq: 6]
          snapshot:
            cid: 6
            id: null
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '???'
        result = yield collection.executeQuery query
        resultArray = yield result.toArray()
        assert.strictEqual resultArray[0].cid, query.snapshot.cid
        assert.strictEqual resultArray[0].data, query.snapshot.data
        yield return
        yield collection.onRemove()
    ###
    it 'Check correctness logic of the "executeQuery" method for "removeBy" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .into collection.collectionFullName()
          .filter '@doc.cid': $eq: 6
          .remove('@doc')
        result = yield collection.executeQuery query
        resultArray = yield result.toArray()
        assert.strictEqual resultArray.length, 0
        yield collection.onRemove()
        yield return
    it 'Check correctness logic of the "parseQuery" method for "find" records', -> # Need mongo version >= 3.4.0
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        query =
          queryType: 'query'
          pipeline: [
            $match: $and: [cid: $gt: 2]
          ,
            $sort:
              cid: -1
              data: 1
          ,
            $limit: 1
          ]
          isCustomReturn: yes
        result = yield collection.executeQuery query
        resultArray = yield result.toArray()
        assert.strictEqual resultArray[0].cid, 4
        assert.strictEqual resultArray[0].data, 'a boat'
        yield collection.onRemove()
        yield return

  describe '#take', ->
    it 'Check correctness logic of the "take" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        result = yield collection.take 'r4'
        assert.instanceOf result, TestRecord
        assert.strictEqual result.cid, 4
        assert.strictEqual result.data, 'a boat'
        yield collection.onRemove()
        yield return

  describe '#takeMany', ->
    it 'Check correctness logic of the "takeMany" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        result = yield collection.takeMany ['e3', 'r4']
        resultArray = yield result.toArray()
        assert.strictEqual resultArray.length, 2
        assert.strictEqual resultArray[1].cid, 4
        assert.strictEqual resultArray[1].data, 'a boat'
        yield collection.onRemove()
        yield return

  describe '#takeAll', ->
    it 'Check correctness logic of the "takeAll" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        result = yield collection.takeAll()
        resultArray = yield result.toArray()
        assert.strictEqual resultArray.length, 4
        assert.strictEqual resultArray[1].cid, 2
        assert.strictEqual resultArray[1].data, 'men'
        yield collection.onRemove()
        yield return

  describe '#includes', ->
    it 'Check correctness logic of the "includes" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        result = yield collection.includes 'w2'
        assert.isTrue result
        result = yield collection.includes 'w3'
        assert.isFalse result
        yield collection.onRemove()
        yield return

  describe '#length', ->
    it 'Check correctness logic of the "length" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        result = yield collection.length()
        assert.strictEqual result, 4
        yield collection.onRemove()
        yield return

  describe '#push', ->
    it 'Check correctness logic of the "push" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', type: 'Test::TestRecord', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        result = yield collection.push testRecord
        assert.isTrue result?
        insertedResult = yield collection.take 'u7'
        assert.strictEqual insertedResult.cid, 7
        assert.strictEqual insertedResult.data, ' :)'
        yield collection.onRemove()
        yield return

  describe '#override', ->
    it 'Check correctness logic of the "override" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        date = new Date()
        testRecord = yield collection.take 'u7'
        assert.isTrue testRecord?
        testRecord.data = ' ;)'
        assert.strictEqual testRecord.data, ' ;)'
        resultObject = yield collection.override testRecord.id, testRecord
        assert.strictEqual resultObject.cid, 7
        assert.strictEqual resultObject.data, ' ;)'
        overridedResult = yield collection.take 'u7'
        assert.strictEqual overridedResult.cid, 7
        assert.strictEqual overridedResult.data, ' ;)'
        yield collection.onRemove()
        yield return

  describe '#patch', ->
    it 'Check correctness logic of the "patch" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'
        date = new Date()
        testRecord = yield collection.take 'u7'
        assert.isTrue testRecord?
        testRecord.data = ' ;-)'
        assert.strictEqual testRecord.data, ' ;-)'
        resultObject = yield collection.override testRecord.id, testRecord
        assert.strictEqual resultObject.cid, 7
        assert.strictEqual resultObject.data, ' ;-)'
        patchedResult = yield collection.take 'u7'
        assert.strictEqual patchedResult.cid, 7
        assert.strictEqual patchedResult.data, ' ;-)'
        yield collection.onRemove()
        yield return

  describe '#remove', ->
    it 'Check correctness logic of the "remove" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        notDeletedResult = yield collection.take 'u7'
        assert.isTrue notDeletedResult?
        yield collection.onRemove()
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST2'
        result = yield collection.remove notDeletedResult.id
        assert.isUndefined result
        yield collection.onRemove()
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST3'
        deletedResult = yield collection.take 'u7'
        assert.isFalse deletedResult?
        yield collection.onRemove()
        yield return

  describe '#createFileWriteStream', ->
    it 'Check correctness logic of the "createFileWriteStream" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        # { mongodb: {dbName}, collection: collectionName } = connectionData
        { mongodb: {dbName} } = connectionData
        collectionName = 'binary-store'
        connection = yield collection.connection
        voDB = yield createConnection "#{dbName}_fs"
        filesCollection = yield voDB.collection "#{collectionName}.files"
        chunksCollection = yield voDB.collection "#{collectionName}.chunks"

        readFileStream = fs.createReadStream "#{__dirname}/test-data/gridfs-test"
        licenseFile = fs.readFileSync "#{__dirname}/test-data/gridfs-test"
        stream = yield collection.createFileWriteStream _id: 'license.test'
        id = stream.id
        promise = LeanRC::Promise.new (resolve, reject)-> stream.once 'finish', resolve
        readFileStream.pipe stream
        yield promise

        chunksQuery = yield chunksCollection.find files_id: id

        # Get all the chunks
        docs = yield chunksQuery.toArray()
        assert.strictEqual docs.length, 1
        assert.strictEqual docs[0].data.toString('hex'), licenseFile.toString('hex')

        filesQuery = yield filesCollection.find _id: id

        docs = yield filesQuery.toArray()
        assert.strictEqual docs.length, 1

        hash = crypto.createHash 'md5'
        hash.update licenseFile
        assert.strictEqual docs[0].md5, hash.digest('hex')

        # make sure we created indexes
        indexes = yield filesCollection.listIndexes().toArray()
        assert.strictEqual indexes.length, 2
        assert.strictEqual indexes[1].name, 'filename_1_uploadDate_1'

        indexes = yield chunksCollection.listIndexes().toArray()
        assert.strictEqual indexes.length, 2
        assert.strictEqual indexes[1].name, 'files_id_1_n_1'
        yield collection.onRemove()
        yield return

  describe '#createFileReadStream', ->
    it 'Check correctness logic of the "createFileWriteStream" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'

        readFileStream = fs.createReadStream "#{__dirname}/test-data/gridfs-test"
        licenseFile = fs.readFileSync "#{__dirname}/test-data/gridfs-test"
        stream = yield collection.createFileWriteStream _id: 'license.test'
        promise = LeanRC::Promise.new (resolve, reject)-> stream.once 'finish', resolve
        readFileStream.pipe stream
        yield promise

        readStream = yield collection.createFileReadStream _id: 'license.test'
        gotData = no
        buffer = Buffer.from []
        promise = LeanRC::Promise.new (resolve, reject)->
          readStream.once 'end', resolve
          readStream.on 'data', (chunk)->
            gotData = yes
            buffer = Buffer.concat [buffer, chunk], buffer.length + chunk.length
        yield promise
        assert.include buffer.toString('utf8'), 'TERMS AND CONDITIONS'
        assert.isTrue gotData
        yield collection.onRemove()
        yield return


  describe '#fileExists', ->
    it 'Check correctness logic of the "createFileWriteStream" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST'

        readFileStream = fs.createReadStream "#{__dirname}/test-data/gridfs-test"
        licenseFile = fs.readFileSync "#{__dirname}/test-data/gridfs-test"
        stream = yield collection.createFileWriteStream _id: 'license.test'
        promise = LeanRC::Promise.new (resolve, reject)-> stream.once 'finish', resolve
        readFileStream.pipe stream
        yield promise
        assert.isTrue yield collection.fileExists _id: 'license.test'
        assert.isFalse yield collection.fileExists _id: 'license.test11'
        yield collection.onRemove()
        yield return
