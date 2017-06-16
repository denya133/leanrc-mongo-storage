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
      yield dbCollection.save id: 1, data: 'three', createdAt: date, updatedAt: date
      date = new Date()
      yield dbCollection.save id: 2, data: 'men', createdAt: date, updatedAt: date
      date = new Date()
      yield dbCollection.save id: 3, data: 'in', createdAt: date, updatedAt: date
      date = new Date()
      yield dbCollection.save id: 4, data: 'a boat', createdAt: date, updatedAt: date
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
        testRecord = TestRecord.new { id: 5, data: '!', createdAt: date, updatedAt: date }, collection
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
            id: '5'
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult
        yield return
    it 'Check correctness logic of method "parseQuery" for "update" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date()
        testRecord = TestRecord.new { id: 5, data: '!!', createdAt: date, updatedAt: date }, collection
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc._id': $eq: 5
          .update testRecord
          .into collection.collectionFullName()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc._id': $eq: 5
          '$update': testRecord
          '$into': collection.collectionFullName()
        correctResult =
          queryType: 'update'
          filter: $and: [_id: $eq: 5]
          snapshot:
            id: '5'
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!!'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult
        yield return
    it 'Check correctness logic of method "parseQuery" for "replace" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        testRecord = TestRecord.new { id: 5, data: '!!', createdAt: date, updatedAt: date }, collection
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc._id': $eq: 5
          .replace testRecord
          .into collection.collectionFullName()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc._id': $eq: 5
          '$replace': testRecord
          '$into': collection.collectionFullName()
        correctResult =
          queryType: 'replace'
          filter: $and: [_id: $eq: 5]
          snapshot:
            id: '5'
            rev: null
            type: 'Test::TestRecord'
            isHidden: no
            createdAt: date.toISOString()
            updatedAt: date.toISOString()
            deletedAt: null
            data: '!!'
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult
        yield return
    it 'Check correctness logic of method "parseQuery" for "remove" record', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        testRecord = TestRecord.new { id: 5, data: '!!', createdAt: date, updatedAt: date }, collection
        query = Test::Query.new()
          .forIn '@doc': collection.collectionFullName()
          .filter '@doc._id': $eq: 5
          .remove()

        result1 = yield collection.parseQuery query
        result2 = yield collection.parseQuery
          '$forIn': '@doc': collection.collectionFullName()
          '$filter': '@doc._id': $eq: 5
          '$remove': yes
        console.log result1
        console.log result2
        correctResult =
          queryType: 'remove'
          filter: $and: [_id: $eq: 5]
        assert.deepEqual result1, correctResult
        assert.deepEqual result2, correctResult
        yield return
    ###
    it 'should get parse query for other with distinct return', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$let':
            k:
              '$forIn':
                'doc1': 'test_samples'
              '$filter':
                '@doc1.test': 'test'
              '$return': 'doc1'
          '$collect':
            l:
              '$forIn':
                'doc2': 'test_samples'
              '$filter':
                '@doc2.test': 'test'
              '$return': 'doc2'
          '$having':
            '$and': [
              '$or': [
                'f': '$eq': '1'
              ,
                '@doc.g': '$eq': '2'
              ]
            ,
              '@doc.h':
                '$not': '$eq': '2'
            ]
          '$sort':
            '@doc.field1': 'ASC'
            '@doc.field2': 'DESC'
          '$limit': 100
          '$offset': 50
          '$distinct': yes
          '$return': '@doc'
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) LET k = FOR doc1 IN test_samples FILTER ((doc1.test == "test")) RETURN doc1 COLLECT l = FOR doc2 IN test_samples FILTER ((doc2.test == "test")) RETURN doc2 INTO test_samples FILTER (((((("f" == "1")) || ((doc.g == "2")))) && (!(doc.h == "2")))) SORT doc.field1 ASC SORT doc.field2 DESC LIMIT 50, 100 RETURN DISTINCT doc'
        yield return
    it 'should get parse query for other with count', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$count': yes
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) INTO test_samples COLLECT WITH COUNT INTO counter RETURN (counter ? counter : 0)'
        yield return
    it 'should get parse query for other with sum', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$sum': '@doc.test'
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) INTO test_samples COLLECT AGGREGATE result = SUM(TO_NUMBER(doc.test)) RETURN result'
        yield return
    it 'should get parse query for other with min', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$min': '@doc.test'
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) INTO test_samples SORT doc.test LIMIT 1 RETURN doc.test'
        yield return
    it 'should get parse query for other with max', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$max': '@doc.test'
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) INTO test_samples SORT doc.test DESC LIMIT 1 RETURN doc.test'
        yield return
    it 'should get parse query for other with average', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$avg': '@doc.test'
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) INTO test_samples COLLECT AGGREGATE result = AVG(TO_NUMBER(doc.test)) RETURN result'
        yield return
    it 'should get parse query for other with return', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = TestCollection.new 'TEST_COLLECTION', Object.assign {}, {delegate: TestRecord}, connectionData
        date = new Date
        result = collection.parseQuery
          '$forIn':
            'doc': 'test_samples'
          '$into': 'test_samples'
          '$join':
            '$and': [
              '@doc.tomatoId': '$eq': '@tomato._key'
            ,
              '@tomato.active': '$eq': yes
            ]
          '$filter':
            '$and': [
              '$or': [
                'c': '$eq': '1'
              ,
                '@doc.b': '$eq': '2'
              ]
            ,
              '@doc.b':
                '$not': '$eq': '2'
            ]
          '$return':
            'doc': '@doc'
        assert.equal result, 'FOR doc IN test_samples FILTER ((doc.tomatoId == tomato._key) && (tomato.active == true)) FILTER (((((("c" == "1")) || ((doc.b == "2")))) && (!(doc.b == "2")))) INTO test_samples RETURN {doc: doc}'
        yield return
  ##

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
