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

createModuleClass = (root = __dirname) ->
  class Test extends LeanRC
    @inheritProtected()
    @include MongoStorage
    @root root
  Test.initialize()

createCollectionClass = (Module) ->
  class Module::MongoCollection extends Module::Collection
    @inheritProtected()
    @include Module::QueryableMixin
    @include Module::MongoCollectionMixin
    @module Module
  Module::MongoCollection.initialize()

createMigrationClass = (Module) ->
  class Module::TestMigration extends Module::Migration
    @inheritProtected()
    @include Module::MongoMigrationMixin
    @module Module
  Module::TestMigration.initialize()

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

describe 'MongoMigrationMixin', ->
  __db = null
  connectionData =
    username: null
    password: null
    host: 'localhost'
    port: '27017'
    default_db: 'just_for_test'
    db: 'just_for_test'
    collection: 'test_migrations'

  # db_url = "mongodb://localhost:27017/just_for_test?authSource=admin"
  { username, password, host, port, default_db } = connectionData
  credentials = if username and password then "#{username}:#{password}@" else ''
  db_url = "mongodb://#{credentials}#{host}:#{port}/#{default_db}?authSource=admin"

  before ->
    co ->
      __db = yield MongoClient.connect db_url
      dbCollection = yield __db.createCollection connectionData.collection
      yield return
  after ->
    co ->
      yield __db.dropCollection connectionData.collection
      __db.close()
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
    it 'Check correctness logic of the "createCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @createCollection testCollectionName
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
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
    it 'Check correctness logic of the "addField" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        date = new Date()
        testRecord = TestRecord.new { id: 'u7', cid: 7, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @addField testCollectionName, 'data1', default: 'testdata1'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
        migration = yield collection.build {}
        spyAddField = sinon.spy migration, 'addField'

        yield migration.up()
        assert.isTrue spyAddField.calledWith testCollectionName, 'data1', default: 'testdata1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.strictEqual (yield testCollection.take 'u7').data1, 'testdata1'

        spyRemoveField = sinon.spy migration, 'removeField'
        yield migration.down()
        assert.isTrue spyRemoveField.calledWith testCollectionName, 'data1'
        testCollection = TestCollection.new testCollectionName,
          Object.assign {}, {delegate: TestRecord}, connectionData, {collection: testCollectionName}
        assert.isFalse (yield testCollection.take 'u7').data1?
        yield return

  describe '#addTimestamps', ->
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
    it 'Check correctness logic of the "addIndex" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @addIndex testCollectionName, ['id', 'cid'], unique: yes, sparse: yes, name: 'testIndex'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
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

  describe '#dropCollection', ->
    it 'Check correctness logic of the "dropCollection" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'
        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @dropCollection testCollectionName
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate:TestMigration}, connectionData
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
        date = new Date()
        testRecord = TestRecord.new { id: 'o9', cid: 9, data: ' :)', createdAt: date, updatedAt: date }, collection
        yield testCollection.push testRecord

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @removeField testCollectionName, 'data1'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
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
    it 'Check correctness logic of the "removeIndex" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionName = 'TestCollection1'

        TestMigration = createMigrationClass Test
        TestMigration.change ()-> @removeIndex testCollectionName, ['id', 'cid'], name: 'testIndex'
        collection = TestCollection.new 'MIGRATIONS', Object.assign {}, {delegate: TestMigration}, connectionData
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

        yield migration.down()
        yield return


# changeCollection
# changeField
# renameField
# renameIndex
# renameCollection
