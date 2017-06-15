{ expect, assert }  = require 'chai'
sinon               = require 'sinon'
_                   = require 'lodash'
MongoStorage        = require.main.require 'lib'
LeanRC              = require 'LeanRC'
{ co }              = LeanRC::Utils
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
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#connection', ->
    it 'Check "connection" property after creating instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = Test::MongoCollection.new 'TestCollection', Object.assign {}, {delegate: TestRecord}, connectionData
        { db: dbName, collection: nativeCollectionName } = connectionData
        connection = yield collection.connection
        assert.isTrue TestCollection[Test::MongoCollection.classVariables['_connection'].pointer]?
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
        collection = Test::MongoCollection.new 'TestCollection', Object.assign {}, {delegate: TestRecord}, connectionData
        nativeCollection = yield collection.collection
        assert.isTrue collection[Test::MongoCollection.instanceVariables['_collection'].pointer]?
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
        collection = Test::MongoCollection.new()
        assert.isTrue no
        yield return

  describe '#onRegister', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        collection = Test::MongoCollection.new 'TestCollection', Object.assign {}, {delegate: TestRecord}, connectionData
        collection.onRegister()
        assert.isTrue TestCollection[Test::MongoCollection.classVariables['_connection'].pointer]?
        yield return

  describe '#push', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#remove', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#take', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#takeMany', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        collection = Test::MongoCollection.new()
        assert.isTrue collection?
        assert.instanceOf collection, Test::MongoCollection
        yield return

  describe '#takeAll', ->
    it 'Create instance of class LeanRC::Collection with MongoCollectionMixin', ->
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
