fs                  = require 'fs'
crypto              = require 'crypto'
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

createModuleClass = (root = __dirname, name = 'Test') ->
  TestModule = class extends LeanRC
    @inheritProtected()
    @root root
    @include MongoStorage
    @initialize()
  Reflect.defineProperty TestModule, 'name', value: name
  TestModule

createCollectionClass = (Module, name = 'MongoCollection') ->
  MongoCollection = class extends Module::Collection
    @inheritProtected()
    @include Module::QueryableCollectionMixin
    @include Module::MongoCollectionMixin
    @module Module
    @initialize()
  Reflect.defineProperty MongoCollection, 'name', value: name
  MongoCollection

createMigrationClass = (Module, name = 'TestMigration') ->
  TestMigration = class extends Module::Migration
    @inheritProtected()
    @include Module::MongoMigrationMixin
    @module Module
    @initialize()
  Reflect.defineProperty TestMigration, 'name', value: name
  TestMigration

createRecordClass = (Module, name = 'TestRecord') ->
  TestRecord = class extends Module::Record
    @inheritProtected()
    @module Module
    @attribute cid: Number, default: -1
    @attribute data: String, default: ''
    @public init: Function,
      default: (args...) ->
        @super args...
        @type = 'Test::TestRecord'
    @initialize()
  Reflect.defineProperty TestRecord, 'name', value: name
  TestRecord

connections = {}


describe 'MongoMigrationMixin', ->
  __db = null
  connectionData =
    mongodb:
      username: null
      password: null
      host: 'localhost'
      port: '27017'
      dbName: 'just_for_test'
    default_db: 'just_for_test'
    collection: 'test_migrations'

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
      dbCollection = yield __db.createCollection connectionData.collection
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
        TestMigration = createMigrationClass Test
        migration = TestMigration.new()
        assert.isTrue migration?
        assert.instanceOf migration, TestMigration
        yield return

  describe '#createCollection', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "createCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @createCollection testCollectionName
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}
        spyCreateCollection = sinon.spy migration, 'createCollection'
        yield migration.up()
        assert.isTrue spyCreateCollection.calledWith testCollectionName
        assert.isTrue (yield __db.collection testCollectionName)?

        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        spyDropCollection = sinon.spy migration, 'dropCollection'
        yield migration.down()
        assert.isTrue spyDropCollection.calledWith testCollectionName
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.isFalse (yield testCollection.take 'u7')?
        yield return

  describe '#createEdgeCollection', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "createEdgeCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollection1Name = 'TestCollection1'
        testCollection2Name = 'TestCollection2'
        testCollectionName = 'TestCollection1_TestCollection2'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @createEdgeCollection testCollection1Name, testCollection2Name
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}

        spyCreateEdgeCollection = sinon.spy migration, 'createEdgeCollection'
        yield migration.up()
        assert.isTrue spyCreateEdgeCollection.calledWith testCollection1Name, testCollection2Name
        assert.isTrue (yield __db.collection testCollectionName)?

        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        spyDropEdgeCollection = sinon.spy migration, 'dropEdgeCollection'
        yield migration.down()
        assert.isTrue spyDropEdgeCollection.calledWith testCollection1Name, testCollection2Name
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.isFalse (yield testCollection.take 'u7')?
        yield return

  describe '#addField', ->
    collection = null
    testCollection = null
    afterEach ->
      co ->
        yield collection.onRemove()  if collection?
        yield testCollection.onRemove()  if testCollection?
        collection = null
        testCollection = null
        yield return
    it 'Check correctness logic of the "addField" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        testCollection.onRegister()
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @addField testCollectionName, 'data1', default: 'testdata1'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}
        spyAddField = sinon.spy migration, 'addField'

        yield migration.up()
        assert.isTrue spyAddField.calledWith testCollectionName, 'data1', default: 'testdata1'
        assert.strictEqual (yield (yield __db.collection testCollectionName).findOne id: 'u7').data1, 'testdata1'

        spyRemoveField = sinon.spy migration, 'removeField'
        yield migration.down()
        assert.isTrue spyRemoveField.calledWith testCollectionName, 'data1'
        assert.isFalse (yield (yield __db.collection testCollectionName).findOne id: 'u7').data1?
        yield return

  describe '#addTimestamps', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "addTimestamps" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'

        date = new Date()
        yield (yield __db.collection testCollectionName).insertOne id: 'i8', cid: 8, data: ' :)', createdAt: date

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @addTimestamps testCollectionName
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}
        spyAddTimestamps = sinon.spy migration, 'addTimestamps'

        yield migration.up()
        assert.isTrue spyAddTimestamps.calledWith testCollectionName
        result = yield (yield __db.collection testCollectionName).findOne id: 'i8'
        assert.isDefined result.createdAt
        assert.isDefined result.updatedAt
        assert.isDefined result.deletedAt

        spyRemoveTimestamps = sinon.spy migration, 'removeTimestamps'
        yield migration.down()
        assert.isTrue spyRemoveTimestamps.calledWith testCollectionName
        result = yield (yield __db.collection testCollectionName).findOne id: 'i8'
        assert.isFalse result.createdAt?
        assert.isFalse result.updatedAt?
        assert.isFalse result.deletedAt?
        yield return

  describe '#addIndex', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "addIndex" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @addIndex testCollectionName, ['id', 'cid'], unique: yes, sparse: yes, name: 'testIndex'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}

        spyAddIndex = sinon.spy migration, 'addIndex'
        yield migration.up()
        assert.isTrue spyAddIndex.calledWith testCollectionName
        assert.isTrue yield (yield __db.collection testCollectionName).indexExists 'testIndex'

        err = null
        try
          yield (yield __db.collection testCollectionName).insertOne id: 'u7', cid: 7, data: ' :)'
        catch error
          err = error
        assert.isTrue err?

        spyRemoveIndex = sinon.spy migration, 'removeIndex'
        yield migration.down()
        assert.isTrue spyRemoveIndex.calledWith testCollectionName
        assert.isFalse yield (yield __db.collection testCollectionName).indexExists 'testIndex'
        yield (yield __db.collection testCollectionName).insertOne id: 'u77', cid: 77, data: ' :)'
        yield return

  describe '#changeField', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "changeField" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        testCollectionName = 'TestCollectionChangeField1'
        testCollection = yield __db.collection testCollectionName
        yield testCollection.insertOne id: 1, cid: 'q1', data: 1, createdAt: new Date()
        yield testCollection.insertOne id: 2, cid: 'w2', data: '12', createdAt: new Date()
        yield testCollection.insertOne id: 3, cid: 'e3', data: {val: 123}, createdAt: new Date()
        yield testCollection.insertOne id: 4, cid: 'r4', data: [1234], createdAt: new Date()
        yield testCollection.insertOne id: 5, cid: 't5', data: [{val: 12345}], createdAt: new Date()
        yield testCollection.insertOne id: 6, cid: 'y6', data: false, createdAt: new Date()
        yield testCollection.insertOne id: 7, cid: 'u7', data: 'false', createdAt: new Date()
        yield testCollection.insertOne id: 8, cid: 'i8', data: null, createdAt: new Date()

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @changeField testCollectionName, 'data', type: TestMigration::SUPPORTED_TYPES.boolean
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}

        spyChangeCollection = sinon.spy migration, 'changeField'
        yield migration.up()
        assert.isTrue spyChangeCollection.calledWith testCollectionName, 'data', type: TestMigration::SUPPORTED_TYPES.boolean
        assert.strictEqual (yield (yield __db.collection testCollectionName).findOne id: 7).data.constructor, Boolean
        assert.strictEqual (yield (yield __db.collection testCollectionName).findOne id: 8).data.constructor, Boolean

        yield migration.down() # Вызывает changeField еще раз, с темы же параметрами что и в первый раз.
        assert.isTrue spyChangeCollection.calledTwice
        yield return

  describe '#renameField', ->
    testCollection = null
    collection = null
    afterEach ->
      co ->
        yield testCollection.onRemove()  if testCollection?
        testCollection = null
        yield collection.onRemove()  if collection?
        collection = null
        yield return
    it 'Check correctness logic of the "renameField" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        testCollection.onRegister()
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @renameField testCollectionName, 'data', 'data1'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}

        spyRenameField = sinon.spy migration, 'renameField'
        yield migration.up()
        assert.isTrue spyRenameField.calledWith testCollectionName, 'data', 'data1'
        result = yield (yield __db.collection testCollectionName).findOne id: 'u7'
        assert.isFalse result.data?
        assert.strictEqual result.data1, ' :)'

        yield migration.down()
        assert.isTrue spyRenameField.calledWith testCollectionName, 'data1', 'data'
        result = yield (yield __db.collection testCollectionName).findOne id: 'u7'
        assert.strictEqual result.data, ' :)'
        assert.isFalse result.data1?
        yield return

  describe '#changeCollection', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "changeCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @changeCollection()
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}

        spyChangeCollection = sinon.spy migration, 'changeCollection'
        yield migration.up()
        assert.isTrue spyChangeCollection.calledOnce

        yield migration.down()
        assert.isTrue spyChangeCollection.calledTwice
        yield return

  describe '#renameCollection', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "renameCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        testCollectionName = 'TestCollectionRename1'
        testCollectionName2 = 'TestCollectionRename2'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @renameCollection testCollectionName, testCollectionName2
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}

        date = new Date()
        yield (yield __db.collection testCollectionName).insertOne id: 'a11', cid: 11, data: ' :)', createdAt: date, updatedAt: date

        spyRenameCollection = sinon.spy migration, 'renameCollection'
        yield migration.up()
        assert.isTrue spyRenameCollection.calledWith testCollectionName, testCollectionName2
        assert.lengthOf (yield __db.listCollections(name: testCollectionName).toArray()), 0
        result = yield (yield __db.collection testCollectionName2).findOne id: 'a11'
        assert.strictEqual result.cid, 11

        yield migration.down()
        assert.isTrue spyRenameCollection.calledWith testCollectionName2, testCollectionName
        assert.lengthOf (yield __db.listCollections(name: testCollectionName2).toArray()), 0
        result = yield (yield __db.collection testCollectionName).findOne id: 'a11'
        assert.strictEqual result.cid, 11
        yield return

  describe '#renameIndex', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "renameIndex" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @renameIndex()
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}

        spyRenameIndex = sinon.spy migration, 'renameIndex'
        yield migration.up()
        assert.isTrue spyRenameIndex.calledOnce

        yield migration.down()
        assert.isTrue spyRenameIndex.calledTwice
        yield return

  describe '#dropCollection', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "dropCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @dropCollection testCollectionName
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}
        spyDropCollection = sinon.spy migration, 'dropCollection'

        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        yield migration.up()
        assert.isTrue spyDropCollection.calledWith testCollectionName
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.isFalse (yield testCollection.take 'u7')?

        yield migration.down() # Вызывает dropCollection еще раз.
        yield return

  describe '#dropEdgeCollection', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "dropEdgeCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollection1Name = 'TestCollection1'
        testCollection2Name = 'TestCollection2'
        testCollectionName = 'TestCollection1_TestCollection2'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @dropEdgeCollection testCollection1Name, testCollection2Name
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}
        spyDropEdgeCollection = sinon.spy migration, 'dropEdgeCollection'

        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        yield migration.up()
        assert.isTrue spyDropEdgeCollection.calledWith testCollection1Name, testCollection2Name
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.isFalse (yield testCollection.take 'u7')?

        yield migration.down() # Вызывает dropEdgeCollection еще раз.
        yield return

  describe '#removeField', ->
    collection = null
    testCollection = null
    afterEach ->
      co ->
        yield collection.onRemove()  if collection?
        collection = null
        yield testCollection.onRemove()  if testCollection?
        testCollection = null
        yield return
    it 'Check correctness logic of the "removeField" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        class TestRecord extends Test::Record
          @inheritProtected()
          @module Test
          @attribute cid: Number, default: -1
          @attribute data: String, default: ''
          @attribute data1: String, default: 'testdata1'
          @public init: Function,
            default: ->
              @super arguments...
              @type = 'Test::TestRecord'
        TestRecord.initialize()
        testCollectionName = 'TestCollection1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        testCollection.onRegister()
        date = new Date()
        testRecord = TestRecord.new { id: 'o9', cid: 9, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @removeField testCollectionName, 'data1'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        migration = yield collection.build {}

        spyRemoveField = sinon.spy migration, 'removeField'
        yield migration.up()
        assert.isTrue spyRemoveField.calledWith testCollectionName, 'data1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.isFalse (yield testCollection.take 'o9').data1?

        yield migration.down() # Вызывает removeField еще раз.
        yield return

  describe '#removeTimestamps', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "removeTimestamps" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'

        date = new Date()
        yield (yield __db.collection testCollectionName).insertOne id: 'p0', cid: 0, data: ' :)', createdAt: date, updatedAt: date

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @removeTimestamps testCollectionName
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}

        spyRemoveTimestamps = sinon.spy migration, 'removeTimestamps'
        yield migration.up()
        assert.isTrue spyRemoveTimestamps.calledWith testCollectionName
        result = yield (yield __db.collection testCollectionName).findOne id: 'p0'
        assert.isFalse result.createdAt?
        assert.isFalse result.updatedAt?
        assert.isFalse result.deletedAt?

        yield migration.down() # Вызывает removeTimestamps еще раз.
        yield return

  describe '#removeIndex', ->
    collection = null
    afterEach ->
      co ->
        yield collection?.onRemove()
        collection = null
        yield return
    it 'Check correctness logic of the "removeIndex" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @removeIndex testCollectionName, ['id', 'cid'], name: 'testIndex'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        collection.onRegister()
        collection.initializeNotifier 'TEST1'
        migration = yield collection.build {}

        yield (yield __db.collection testCollectionName).ensureIndex {id: 1, cid: 1},
          unique: yes
          sparse: yes
          name: 'testIndex'

        spyRemoveIndex = sinon.spy migration, 'removeIndex'
        yield migration.up()
        assert.isTrue spyRemoveIndex.calledWith testCollectionName, ['id', 'cid'], name: 'testIndex'
        assert.isFalse yield (yield __db.collection testCollectionName).indexExists 'testIndex'
        yield (yield __db.collection testCollectionName).insertOne id: 'u777', cid: 777, data: ' :)'

        yield migration.down() # Вызывает removeIndex еще раз.
        yield return
