{ expect, assert }  = require 'chai'
sinon               = require 'sinon'
_                   = require 'lodash'
MongoStorage        = require.main.require 'lib'
LeanRC              = require '@leansdk/leanrc/lib'
{ co }              = LeanRC::Utils
{ MongoClient }     = require 'mongodb'

createModuleClass = (root = __dirname, name = 'Test') ->
  TestModule = class extends LeanRC
    @inheritProtected()
    @root root
    @include MongoStorage
    @initialize()
  Reflect.defineProperty TestModule, 'name', value: name
  TestModule

createCollectionClass = (Module, name = 'TestCollection') ->
  TestCollection = class extends Module::Collection
    @inheritProtected()
    @module Module
    @initialize()
  Reflect.defineProperty TestCollection, 'name', value: name
  TestCollection

createRecordClass = (Module, name = 'TestRecord') ->
  TestRecord = class extends Module::Record
    @inheritProtected()
    @module Module
    @attribute data: String, { default: '' }
    @initialize()
  Reflect.defineProperty TestRecord, 'name', value: name
  TestRecord


describe 'MongoCursor', ->
  db = null

  before ->
    co ->
      db_url = "mongodb://localhost:27017/just_for_test?authSource=admin"
      db = yield MongoClient.connect db_url
      dbCollection = yield db.createCollection 'test_thames_travel'
      date = new Date().toISOString()
      yield dbCollection.save id: 1, type: 'Test::TestRecord', data: 'three', createdAt: date, updatedAt: date
      date = new Date().toISOString()
      yield dbCollection.save id: 2, type: 'Test::TestRecord', data: 'men', createdAt: date, updatedAt: date
      date = new Date().toISOString()
      yield dbCollection.save id: 3, type: 'Test::TestRecord', data: 'in', createdAt: date, updatedAt: date
      date = new Date().toISOString()
      yield dbCollection.save id: 4, type: 'Test::TestRecord', data: 'a boat', createdAt: date, updatedAt: date
      yield return
  after ->
    co ->
      yield db.dropCollection 'test_thames_travel'
      db.close()
      yield return

  describe '.new', ->
    it 'Create MongoCursor instance with two valid params', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find()
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        assert.isTrue cursor?, 'Cursor not defined'
        assert.instanceOf cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollection
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], testCollectionInstance
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor
        yield return
    it 'Create MongoCursor instance with only Collection instance as param', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        cursor = Test::MongoCursor.new testCollectionInstance
        assert.isTrue cursor?, 'Cursor not defined'
        assert.instanceOf cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollection
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], testCollectionInstance
        # assert.isFalse cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        yield return
    it 'Create MongoCursor instance with only NativeCursor as param', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find()
        cursor = Test::MongoCursor.new null, nativeCursor
        assert.isTrue cursor?, 'Cursor not defined'
        # assert.isFalse cursor[Test::MongoCursor.instanceVariables['_collection'].pointer]?
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor
        yield return
    it 'Create MongoCursor instance without params', ->
      co ->
        Test = createModuleClass()
        cursor = Test::MongoCursor.new()
        assert.isTrue cursor?, 'Cursor not defined'
        # assert.isFalse cursor[Test::MongoCursor.instanceVariables['_collection'].pointer]?
        # assert.isFalse cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        yield return
    # it 'Create MongoCursor instance with invalid params', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = null
    #     err = null
    #     try
    #       cursor = Test::MongoCursor.new "It's wrong parameter", true
    #     catch error
    #       err = error
    #     # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
    #     yield return

  describe '#setCollection', ->
    it 'Setup collection on created MongoCursor instance with valid params', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        cursor = Test::MongoCursor.new()
        cursor.setCollection testCollectionInstance
        assert.instanceOf cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], TestCollection
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], testCollectionInstance
        yield return
    it 'Use method setCollection for change used collection', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION_1',
          delegate: TestRecord
        testCollectionInstance2 = TestCollection.new 'TEST_COLLECTION_2',
          delegate: TestRecord
        cursor = Test::MongoCursor.new testCollectionInstance
        cursor.setCollection testCollectionInstance2
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_collection'].pointer], testCollectionInstance2
        yield return
    # it 'Setup collection on created MongoCursor instance without params', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       cursor.setCollection()
    #     catch error
    #       err = error
    #     # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
    #     yield return
    # it 'Setup collection on created MongoCursor instance with invalid params', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       cursor.setCollection "It's wrong parameter"
    #     catch error
    #       err = error
    #     # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
    #     yield return

  describe '#setIterable', ->
    it 'Setup cursor on created MongoCursor instance with valid params', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find()
        cursor = Test::MongoCursor.new()
        cursor.setIterable nativeCursor
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor
        yield return
    it 'Use method setIterable for change used cursor', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find()
        nativeCursor2 = yield dbCollection.find()
        cursor = Test::MongoCursor.new null, nativeCursor
        cursor.setIterable nativeCursor2
        assert.isTrue cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer]?
        assert.strictEqual cursor[Test::MongoCursor.instanceVariables['_cursor'].pointer], nativeCursor2
        yield return
    # it 'Setup cursor on created MongoCursor instance without params', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       cursor.setIterable()
    #     catch error
    #       err = error
    #     # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
    #     yield return
    # it 'Setup cursor on created MongoCursor instance with invalid params', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       cursor.setIterable true
    #     catch error
    #       err = error
    #     # assert.isTrue err? # @TODO Uncomment it after will complete implementation functional for checking types of arguments
    #     yield return

  describe '#hasNext', ->
    it 'Check correctness logic of the "hasNext" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().limit 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        assert.isTrue yield cursor.hasNext()
        yield cursor.next()
        assert.isFalse yield cursor.hasNext()
        yield return
    # it 'Check correctness logic of the "hasNext" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.hasNext()
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#next', ->
    it 'Use next manually', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
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
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
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
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
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

  describe '#toArray', ->
    it 'Check correctness logic of the "toArray" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        nativeRecords = yield nativeCursor.toArray()
        nativeCursor2 = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor2
        records = yield cursor.toArray()
        assert.strictEqual records.length, nativeRecords.length, 'Counts of input and output data are different'
        assert.instanceOf records, Array, 'Counts of input and output data are different'
        for record, index in records
          assert.instanceOf record, TestRecord, "Record #{index} has incorrect Class"
          assert.strictEqual record.data, nativeRecords[index].data, "Record #{index} `data` is incorrect"
        yield return

  describe '#close', ->
    it 'Check correctness logic of the "close" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        assert.isTrue yield cursor.hasNext()
        yield cursor.close()
        assert.isFalse yield cursor.hasNext()
        yield cursor.close()
        yield return
    it 'Check correctness logic of the "close" function when cursor haven\'t Collection instance', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        assert.isTrue yield cursor.hasNext()
        yield cursor.close()
        assert.isFalse yield cursor.hasNext()
        yield cursor.close()
        yield return
    # it 'Check correctness logic of the "close" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.close()
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#count', ->
    it 'Check correctness logic of the "count" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TEST_COLLECTION',
          delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        assert.strictEqual (yield cursor.count()), (yield nativeCursor.count()), 'Count works incorrectly'
        yield return
    # it 'Check correctness logic of the "count" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.count()
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#forEach', ->
    it 'Check correctness logic of the "forEach" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        spyLambda = sinon.spy -> yield return
        yield cursor.forEach spyLambda
        assert.isTrue spyLambda.called, 'Lambda never called'
        assert.strictEqual spyLambda.callCount,       4, 'Lambda calls are not match'
        assert.strictEqual spyLambda.args[0][0].data, 'three', 'Lambda 1st call is not match'
        assert.strictEqual spyLambda.args[0][1],      0, 'Lambda 1st call is not match'
        assert.strictEqual spyLambda.args[1][0].data, 'men', 'Lambda 2nd call is not match'
        assert.strictEqual spyLambda.args[1][1],      1, 'Lambda 2nd call is not match'
        assert.strictEqual spyLambda.args[2][0].data, 'in', 'Lambda 3rd call is not match'
        assert.strictEqual spyLambda.args[2][1],      2, 'Lambda 3rd call is not match'
        assert.strictEqual spyLambda.args[3][0].data, 'a boat', 'Lambda 4th call is not match'
        assert.strictEqual spyLambda.args[3][1],      3, 'Lambda 4th call is not match'
        yield return
    it 'Check correctness logic of the "forEach" function without Record class', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        spyLambda = sinon.spy -> yield return
        yield cursor.forEach spyLambda
        assert.isTrue spyLambda.called, 'Lambda never called'
        assert.strictEqual spyLambda.callCount,       4, 'Lambda calls are not match'
        assert.strictEqual spyLambda.args[0][0].data, 'three', 'Lambda 1st call is not match'
        assert.strictEqual spyLambda.args[0][1],      0, 'Lambda 1st call is not match'
        assert.strictEqual spyLambda.args[1][0].data, 'men', 'Lambda 2nd call is not match'
        assert.strictEqual spyLambda.args[1][1],      1, 'Lambda 2nd call is not match'
        assert.strictEqual spyLambda.args[2][0].data, 'in', 'Lambda 3rd call is not match'
        assert.strictEqual spyLambda.args[2][1],      2, 'Lambda 3rd call is not match'
        assert.strictEqual spyLambda.args[3][0].data, 'a boat', 'Lambda 4th call is not match'
        assert.strictEqual spyLambda.args[3][1],      3, 'Lambda 4th call is not match'
        yield return
    # it 'Check correctness logic of the "forEach" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.forEach -> yield return
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#map', ->
    it 'Check correctness logic of the "map" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        records = yield cursor.map (record, index) ->
          record.data = "#{index + 1}.#{record.data}"
          yield Test::Promise.resolve record
        assert.lengthOf records, 4, 'Records count is not match'
        assert.strictEqual records[0].data, '1.three', '1st record is not match'
        assert.strictEqual records[1].data, '2.men', '2nd record is not match'
        assert.strictEqual records[2].data, '3.in', '3rd record is not match'
        assert.strictEqual records[3].data, '4.a boat', '4th record is not match'
        yield return
    it 'Check correctness logic of the "map" function without Record class', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        records = yield cursor.map (record, index) ->
          record.data = "#{index + 1}.#{record.data}"
          yield Test::Promise.resolve record
        assert.lengthOf records, 4, 'Records count is not match'
        assert.strictEqual records[0].data, '1.three', '1st record is not match'
        assert.strictEqual records[1].data, '2.men', '2nd record is not match'
        assert.strictEqual records[2].data, '3.in', '3rd record is not match'
        assert.strictEqual records[3].data, '4.a boat', '4th record is not match'
        yield return
    # it 'Check correctness logic of the "map" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.map -> yield return
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#filter', ->
    it 'Check correctness logic of the "filter" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        records = yield cursor.filter (record, index) ->
          yield Test::Promise.resolve record.data.length > 3 and index < 3
        assert.lengthOf records, 1, 'Records count is not match'
        assert.strictEqual records[0].data, 'three', '1st record is not match'
        yield return
    it 'Check correctness logic of the "filter" function without Record class', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        records = yield cursor.filter (record, index) ->
          yield Test::Promise.resolve record.data.length > 3 and index < 3
        assert.lengthOf records, 1, 'Records count is not match'
        assert.strictEqual records[0].data, 'three', '1st record is not match'
        yield return
    # it 'Check correctness logic of the "filter" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.filter -> yield return
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#find', ->
    it 'Check correctness logic of the "find" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        findedRecord = yield cursor.find (record, index) ->
          yield Test::Promise.resolve record.data.length > 3 and index < 3
        assert.isTrue findedRecord?
        assert.strictEqual findedRecord.data, 'three', 'Record is not match'
        notFindedRecord = yield cursor.find (record, index) ->
          yield Test::Promise.resolve record.data.length > 3 and index > 5
        assert.isNull notFindedRecord
        yield return
    it 'Check correctness logic of the "find" function without Record class', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        findedRecord = yield cursor.find (record, index) ->
          yield Test::Promise.resolve record.data.length > 3 and index < 3
        assert.isTrue findedRecord?
        assert.strictEqual findedRecord.data, 'three', 'Record is not match'
        notFindedRecord = yield cursor.find (record, index) ->
          yield Test::Promise.resolve record.data.length > 3 and index > 5
        assert.isNull notFindedRecord
        yield return
    # it 'Check correctness logic of the "find" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.find -> yield return
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#compact', ->
    it 'Check correctness logic of the "compact" function', ->
      co ->
        # ikos:
        # Способа заставить монгу вернуть курсор объектов вида [null, 1, 2, {test: 'data'}, null, undefined] я не нашел.
        # Формат возвращаемых данных всегда объект, с id и както дополнительным полем [{_id: 1, value: 'data'}]
        yield return
    # it 'Check correctness logic of the "compact" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.compact()
    #     catch error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#reduce', ->
    it 'Check correctness logic of the "reduce" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        records = yield cursor.reduce (accumulator, item, index) ->
          accumulator["#{index + 1}.#{item.data}"] = item
          yield Test::Promise.resolve accumulator
        , {}
        assert.strictEqual records['1.three'].data, 'three', '1st record is not match'
        assert.strictEqual records['2.men'].data, 'men', '2nd record is not match'
        assert.strictEqual records['3.in'].data, 'in', '3rd record is not match'
        assert.strictEqual records['4.a boat'].data, 'a boat', '4th record is not match'
        yield return
    it 'Check correctness logic of the "reduce" function without Record class', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        records = yield cursor.reduce (accumulator, item, index) ->
          accumulator["#{index + 1}.#{item.data}"] = item
          yield Test::Promise.resolve accumulator
        , {}
        assert.strictEqual records['1.three'].data, 'three', '1st record is not match'
        assert.strictEqual records['2.men'].data, 'men', '2nd record is not match'
        assert.strictEqual records['3.in'].data, 'in', '3rd record is not match'
        assert.strictEqual records['4.a boat'].data, 'a boat', '4th record is not match'
        yield return
    # it 'Check correctness logic of the "reduce" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.reduce ((p, i)-> p.push i; yield return p), []
    #     catch error
    #       console.log '>>>??? 111', error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '#first', ->
    it 'Check correctness logic of the "first" function', ->
      co ->
        Test = createModuleClass()
        TestCollection = createCollectionClass Test
        TestRecord = createRecordClass Test
        testCollectionInstance = TestCollection.new 'TestCollection', delegate: TestRecord
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new testCollectionInstance, nativeCursor
        firstRecord = yield cursor.first()
        assert.isTrue firstRecord?
        assert.strictEqual firstRecord.data, 'three', '1st record is not match'
        err = null
        firstRecord = yield cursor.first()
        assert.isTrue cursor.isClosed
        nativeCursor2 = yield dbCollection.find().sort id: 1
        cursor.setIterable nativeCursor2
        firstRecord = yield cursor.first()
        assert.isTrue firstRecord?
        assert.strictEqual firstRecord.data, 'three', '1st record is not match'
        secondFirstRecord = yield cursor.first()
        assert.isNull secondFirstRecord
        # assert.deepEqual firstRecord, secondFirstRecord
        assert.isTrue cursor.isClosed
        yield return
    it 'Check correctness logic of the "first" function without Record class', ->
      co ->
        Test = createModuleClass()
        dbCollection = db.collection "test_thames_travel"
        nativeCursor = yield dbCollection.find().sort id: 1
        cursor = Test::MongoCursor.new null, nativeCursor
        firstRecord = yield cursor.first()
        assert.isTrue firstRecord?
        assert.strictEqual firstRecord.data, 'three', '1st record is not match'
        secondFirstRecord = yield cursor.first()
        assert.isNull secondFirstRecord
        # assert.deepEqual firstRecord, secondFirstRecord
        assert.isTrue cursor.isClosed
        yield return
    # it 'Check correctness logic of the "first" function when cursor haven\'t native cursor', ->
    #   co ->
    #     Test = createModuleClass()
    #     cursor = Test::MongoCursor.new()
    #     err = null
    #     try
    #       yield cursor.first()
    #     catch error
    #       console.log '>>>??? 222', error
    #       err = error
    #     assert.isTrue err?
    #     yield return

  describe '.restoreObject', ->
    it 'Check correctness logic of the "restoreObject" static function', ->
      co ->
        Test = createModuleClass()
        err = null
        try
          yield Test::MongoCursor.restoreObject()
        catch error
          err = error
        assert.isTrue err?
        yield return

  describe '.replicateObject', ->
    it 'Check correctness logic of the "replicateObject" static function', ->
      co ->
        Test = createModuleClass()
        err = null
        try
          yield Test::MongoCursor.replicateObject()
        catch error
          err = error
        assert.isTrue err?
        yield return
