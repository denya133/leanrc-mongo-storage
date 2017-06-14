{ expect, assert }  = require 'chai'
sinon               = require 'sinon'
_                   = require 'lodash'
MongoStorage        = require.main.require 'lib'
LeanRC              = require 'LeanRC'
{ co }              = LeanRC::Utils
{ MongoClient }     = require 'mongodb'

createModule = (root = __dirname) ->
  class Test extends LeanRC
    @inheritProtected()
    @include MongoStorage
    @root root
  Test.initialize()

createCollection = (Module) ->
  class TestCollection extends Module::Collection
    @inheritProtected()
    @module Module
  TestCollection.initialize()

createRecord = (Module) ->
  class TestRecord extends Module::Record
    @inheritProtected()
    @module Module
    @attribute data: String, default: ''
  TestRecord.initialize()


describe 'MongoCursor', ->
  db = null

  before ->
    co ->
      db_url = "mongodb://localhost:27017/just_for_test?authSource=admin"
      db = yield MongoClient.connect db_url
      collection = yield db.createCollection 'test_thames_travel'
      date = new Date()
      collection.save id: 1, data: 'three', createdAt: date, updatedAt: date
      date = new Date()
      collection.save id: 2, data: 'men', createdAt: date, updatedAt: date
      date = new Date()
      collection.save id: 3, data: 'in', createdAt: date, updatedAt: date
      date = new Date()
      collection.save id: 4, data: 'a boat', createdAt: date, updatedAt: date
      yield return
  after ->
    co ->
      yield db.dropCollection 'test_thames_travel'
      db.close()
      yield return

  describe '.new', ->
    it 'Create MongoCursor instance with two valid params', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestCollectionInstance = TestCollection.new()
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find()
        cursor = Test::MongoCursor.new TestCollectionInstance, nativeCursor
        assert.isTrue cursor?, 'Cursor not defined'
        assert.instanceOf cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], Test::Collection
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollectionInstance
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor
        yield return
    it 'Create MongoCursor instance with only Collection instance as param', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestCollectionInstance = TestCollection.new()
        cursor = Test::MongoCursor.new TestCollectionInstance
        assert.isTrue cursor?, 'Cursor not defined'
        assert.instanceOf cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], Test::Collection
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollectionInstance
        assert.isFalse cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        yield return
    it 'Create MongoCursor instance with only NativeCursor as param', ->
      co ->
        Test = createModule()
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find()
        cursor = Test::MongoCursor.new null, nativeCursor
        assert.isTrue cursor?, 'Cursor not defined'
        assert.isFalse cursor[Test::MongoCursor.instanceVariables['_collection'].pointer]?
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor
        yield return
    it 'Create MongoCursor instance without params', ->
      co ->
        Test = createModule()
        cursor = Test::MongoCursor.new()
        assert.isTrue cursor?, 'Cursor not defined'
        assert.isFalse cursor[Test::MongoCursor.instanceVariables['_collection'].pointer]?
        assert.isFalse cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        yield return
    it 'Create MongoCursor instance with invalid params', ->
      co ->
        Test = createModule()
        cursor = null
        err = null
        try
          cursor = Test::MongoCursor.new "It's wrong parameter", true
        catch error
          err = error
        # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
        yield return

  describe '#setCollection', ->
    it 'Setup collection on created MongoCursor instance with valid params', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestCollectionInstance = TestCollection.new()
        cursor = Test::MongoCursor.new()
        cursor.setCollection TestCollectionInstance
        assert.instanceOf cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], Test::Collection
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollectionInstance
        yield return
    it 'Use method setCollection for change used collection', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestCollectionInstance = TestCollection.new()
        TestCollectionInstance2 = TestCollection.new()
        cursor = Test::MongoCursor.new TestCollectionInstance
        cursor.setCollection TestCollectionInstance2
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollectionInstance2
        yield return
    it 'Setup collection on created MongoCursor instance without params', ->
      co ->
        Test = createModule()
        cursor = Test::MongoCursor.new()
        err = null
        try
          cursor.setCollection()
        catch error
          err = error
        # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
        yield return
    it 'Setup collection on created MongoCursor instance with invalid params', ->
      co ->
        Test = createModule()
        cursor = Test::MongoCursor.new()
        err = null
        try
          cursor.setCollection "It's wrong parameter"
        catch error
          err = error
        # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
        yield return

  describe '#setCursor', ->
    it 'Setup cursor on created MongoCursor instance with valid params', ->
      co ->
        Test = createModule()
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find()
        cursor = Test::MongoCursor.new()
        cursor.setCursor nativeCursor
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor
        yield return
    it 'Use method setCursor for change used cursor', ->
      co ->
        Test = createModule()
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find()
        nativeCursor2 = yield collection.find()
        cursor = Test::MongoCursor.new nativeCursor
        cursor.setCursor nativeCursor2
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor2
        yield return
    it 'Setup cursor on created MongoCursor instance without params', ->
      co ->
        Test = createModule()
        cursor = Test::MongoCursor.new()
        err = null
        try
          cursor.setCursor()
        catch error
          err = error
        # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
        yield return
    it 'Setup cursor on created MongoCursor instance with invalid params', ->
      co ->
        Test = createModule()
        cursor = Test::MongoCursor.new()
        err = null
        try
          cursor.setCursor true
        catch error
          err = error
        # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
        yield return

  describe '#hasNext', ->
    it 'Check correctness of the hasNext function', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestCollectionInstance = TestCollection.new()
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find().limit 1
        cursor = Test::MongoCursor.new TestCollectionInstance, nativeCursor
        assert.isTrue yield cursor.hasNext()
        yield cursor.next()
        assert.isFalse yield cursor.hasNext()
        yield return

  describe '#next', ->
    it 'Use next manually', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestRecord = createRecord Test
        TestCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find().sort id: 1
        cursor = Test::MongoCursor.new TestCollectionInstance, nativeCursor
        assert.strictEqual (yield cursor.next()).data, 'three', 'First item is incorrect'
        assert.strictEqual (yield cursor.next()).data, 'men', 'Second item is incorrect'
        assert.strictEqual (yield cursor.next()).data, 'in', 'Third item is incorrect'
        assert.strictEqual (yield cursor.next()).data, 'a boat', 'Fourth item is incorrect'
        assert.isFalse (yield cursor.next())?, 'Unexpected item is present'
        err = null
        try
          yield cursor.next()
        catch error
          err = error
        assert.isTrue err?
        yield return
    it 'Use next automatic', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestRecord = createRecord Test
        TestCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find().sort id: 1
        cursor = Test::MongoCursor.new TestCollectionInstance, nativeCursor
        expectedData = ['three', 'men', 'in', 'a boat']
        index = 0
        while (data = (yield cursor.next())?.data)?
          assert.strictEqual data, expectedData[index++]
        err = null
        try
          yield cursor.next()
        catch error
          err = error
        assert.isTrue err?
        yield return
    it 'Use next automatic (with hasNext)', ->
      co ->
        Test = createModule()
        TestCollection = createCollection Test
        TestRecord = createRecord Test
        TestCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        collection = db.collection "test_thames_travel"
        nativeCursor = yield collection.find().sort id: 1
        cursor = Test::MongoCursor.new TestCollectionInstance, nativeCursor
        expectedData = ['three', 'men', 'in', 'a boat']
        index = 0
        while yield cursor.hasNext()
          assert.strictEqual (yield cursor.next()).data, expectedData[index++]
        err = null
        try
          yield cursor.next()
        catch error
          err = error
        assert.isTrue err?
        yield return

  # describe '#toArray', ->
  #   it 'should get array from cursor', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       array = db._query 'FOR item IN test_thames_travel SORT item._key RETURN item'
  #       .toArray()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       records = yield cursor.toArray()
  #       assert.equal records.length, array.length, 'Counts of input and output data are different'
  #       for record, index in records
  #         assert.instanceOf record, TestRecord, "Record #{index} is incorrect"
  #         assert.equal record.data, array[index].data, "Record #{index} `data` is incorrect"
  #       return
  # describe '#close', ->
  #   it 'should remove records from cursor', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       assert.isTrue (yield cursor.hasNext()), 'There is no next value'
  #       yield cursor.close()
  #       assert.isFalse (yield cursor.hasNext()), 'There is something else'
  #       return
  # describe '#count', ->
  #   it 'should count records in cursor', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       assert.equal (yield cursor.count()), 4, 'Count works incorrectly'
  #       return
  # describe '#forEach', ->
  #   it 'should call lambda in each record in cursor', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       spyLambda = sinon.spy -> yield return
  #       yield cursor.forEach spyLambda
  #       assert.isTrue spyLambda.called, 'Lambda never called'
  #       assert.equal spyLambda.callCount, 4, 'Lambda calls are not match'
  #       assert.equal spyLambda.args[0][0].data, 'three', 'Lambda 1st call is not match'
  #       assert.equal spyLambda.args[1][0].data, 'men', 'Lambda 2nd call is not match'
  #       assert.equal spyLambda.args[2][0].data, 'in', 'Lambda 3rd call is not match'
  #       assert.equal spyLambda.args[3][0].data, 'a boat', 'Lambda 4th call is not match'
  #       return
  # describe '#map', ->
  #   it 'should map records using lambda', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       records = yield cursor.map (record) ->
  #         record.data = '+' + record.data + '+'
  #         yield Test::Promise.resolve record
  #       assert.lengthOf records, 4, 'Records count is not match'
  #       assert.equal records[0].data, '+three+', '1st record is not match'
  #       assert.equal records[1].data, '+men+', '2nd record is not match'
  #       assert.equal records[2].data, '+in+', '3rd record is not match'
  #       assert.equal records[3].data, '+a boat+', '4th record is not match'
  #       return
  # describe '#filter', ->
  #   it 'should filter records using lambda', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       records = yield cursor.filter (record) ->
  #         yield Test::Promise.resolve record.data.length > 3
  #       assert.lengthOf records, 2, 'Records count is not match'
  #       assert.equal records[0].data, 'three', '1st record is not match'
  #       assert.equal records[1].data, 'a boat', '2nd record is not match'
  #       return
  # describe '#find', ->
  #   before ->
  #     collection = db._create 'test_collection'
  #     date = new Date()
  #     collection.save id: 1, name: 'Jerome', createdAt: date, updatedAt: date
  #     date = new Date()
  #     collection.save id: 1, name: 'George', createdAt: date, updatedAt: date
  #     date = new Date()
  #     collection.save id: 1, name: 'Harris', createdAt: date, updatedAt: date
  #   after ->
  #     db._drop 'test_collection'
  #   it 'should find record using lambda', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute name: String, { default: 'Unknown' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db.test_collection.all()
  #       record = yield cursor.find (record) ->
  #         yield Test::Promise.resolve record.name is 'George'
  #       assert.equal record.name, 'George', 'Record is not match'
  #       record = yield cursor.find (record) ->
  #         yield Test::Promise.resolve record.name is 'Marvel'
  #       assert.isNull record
  #       return
  # describe '#compact', ->
  #   before ->
  #     collection = db._create 'test_collection'
  #     date = new Date()
  #     collection.save id: 1, data: 'men', createdAt: date, updatedAt: date
  #     date = new Date()
  #     collection.save id: 1, data: null, createdAt: date, updatedAt: date
  #     date = new Date()
  #     collection.save id: 1, data: 'a boat', createdAt: date, updatedAt: date
  #   after ->
  #     db._drop 'test_collection'
  #   it 'should get non-empty records from cursor', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_collection SORT item._key RETURN item.data ? item : null
  #       '''
  #       records = yield cursor.compact()
  #       assert.lengthOf records, 2, 'Records count not match'
  #       assert.equal records[0].data, 'men', '1st record is not match'
  #       assert.equal records[1].data, 'a boat', '2nd record is not match'
  #       return
  # describe '#reduce', ->
  #   it 'should reduce records using lambda', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       records = yield cursor.reduce (accumulator, item) ->
  #         accumulator[item.data] = item
  #         yield Test::Promise.resolve accumulator
  #       , {}
  #       assert.equal records['three'].data, 'three', '1st record is not match'
  #       assert.equal records['men'].data, 'men', '2nd record is not match'
  #       assert.equal records['in'].data, 'in', '3rd record is not match'
  #       assert.equal records['a boat'].data, 'a boat', '4th record is not match'
  #       return
  # describe '#first', ->
  #   before ->
  #     collection = db._create 'test_collection'
  #     date = new Date()
  #     collection.save id: 1, data: 'Jerome', createdAt: date, updatedAt: date
  #     date = new Date()
  #     collection.save id: 1, data: 'George', createdAt: date, updatedAt: date
  #     date = new Date()
  #     collection.save id: 1, data: 'Harris', createdAt: date, updatedAt: date
  #   after ->
  #     db._drop 'test_collection'
  #   it 'should get first record from cursor', ->
  #     co ->
  #       class Test extends LeanRC
  #         @inheritProtected()
  #         @include MongoStorage
  #         @root __dirname
  #       Test.initialize()
  #       class TestRecord extends Test::Record
  #         @inheritProtected()
  #         @module Test
  #         @attribute data: String, { default: '' }
  #       TestRecord.initialize()
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_thames_travel SORT item._key RETURN item
  #       '''
  #       record = yield cursor.first()
  #       assert.equal record.data, 'three', '1st record is not match'
  #       cursor = Test::MongoCursor.new TestRecord, db._query '''
  #         FOR item IN test_collection SORT item._key RETURN item
  #       '''
  #       record = yield cursor.first()
  #       assert.equal record.data, 'Jerome', 'Another 1st record is not match'
  #       return
