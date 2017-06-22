

_             = require 'lodash'


###
```coffee
module.exports = (Module)->
  class CreateUsersCollectionMigration extends Module::Migration
    @inheritProtected()
    @include Module::MongoMigrationMixin # в этом миксине должны быть реализованы платформозависимые методы, которые будут посылать нативные запросы к реальной базе данных

    @module Module

    @up ->
      yield @createCollection 'users'
      yield @addField 'users', name, 'string'
      yield @addField 'users', description, 'text'
      yield @addField 'users', createdAt, 'date'
      yield @addField 'users', updatedAt, 'date'
      yield @addField 'users', deletedAt, 'date'
      yield return

    @down ->
      yield @dropCollection 'users'
      yield return

  return CreateUsersCollectionMigration.initialize()
```

Это эквивалентно

```coffee
module.exports = (Module)->
  class CreateUsersCollectionMigration extends Module::Migration
    @inheritProtected()
    @include Module::MongoMigrationMixin # в этом миксине должны быть реализованы платформозависимые методы, которые будут посылать нативные запросы к реальной базе данных

    @module Module

    @change ->
      @createCollection 'users'
      @addField 'users', name, 'string'
      @addField 'users', description, 'text'
      @addField 'users', createdAt, 'date'
      @addField 'users', updatedAt, 'date'
      @addField 'users', deletedAt, 'date'


  return CreateUsersCollectionMigration.initialize()
```
###

# Миксин объявляет реализации для виртуальных методов основного Migration класса
# миксин должен содержать нативный платформозависимый код для обращения к релаьной базе данных на понятном ей языке.

module.exports = (Module)->
  Module.defineMixin Module::Migration, (BaseClass) ->
    class MongoMigrationMixin extends BaseClass
      @inheritProtected()

      @public @async createCollection: Function,
        default: (collectionName, options)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          yield voDB.createCollection collectionName, options
          yield return

      @public @async createEdgeCollection: Function,
        default: (collectionName1, collectionName2, options)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          yield voDB.createCollection "#{collectionName1}_#{collectionName2}", options
          yield return

      @public @async addField: Function,
        default: (collectionName, fieldName, options = {})->
          if options.default?
            if _.isNumber(options.default) or _.isBoolean(options.default)
              initial = options.default
            else if _.isDate options.default
              initial = options.default.toISOString()
            else if _.isString options.default
              initial = "#{options.default}"
            else
              initial = null
          else
            initial = null
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          yield collection.updateMany {},
            $set:
              "#{fieldName}": initial
          , w: 1
          yield return

      @public @async addIndex: Function,
        default: (collectionName, fieldNames, options)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          indexFields = {}
          fieldNames.forEach (fieldName)->
            indexFields[fieldName] = 1
          yield collection.ensureIndex indexFields,
            unique: options.unique
            sparse: options.sparse
            background: options.background
            name: options.name
          yield return

      @public @async addTimestamps: Function,
        default: (collectionName, options)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          yield collection.updateMany {},
            $set:
              createdAt: null
              updatedAt: null
              deletedAt: null
          , w: 1
          yield return

      @public @async changeCollection: Function,
        default: (name, options)->
          # not supported in MongoDB because a collection can't been modified
          yield return

      @public @async changeField: Function,
        default: (collectionName, fieldName, options)->
          {
            json
            binary
            boolean
            date
            datetime
            decimal
            float
            integer
            primary_key
            string
            text
            time
            timestamp
            array
            hash
          } = Module::Migration::SUPPORTED_TYPES
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          cursor = yield collection.find().batchSize(1)
          while yield cursor.hasNext()
            document = yield cursor.next()
            newValue = switch options.type
              when boolean
                Boolean document[fieldName]
              when decimal, float, integer
                Number document[fieldName]
              when string, text, primary_key, binary, array
                JSON.stringify document[fieldName]
              when json, hash
                JSON.parse String document[fieldName]
              when date, datetime
                (new Date document[fieldName]).toISOString()
              when time, timestamp
                Number new Date document[fieldName]
            yield collection.updateOne
              _id: document._id
            ,
              $set: "#{fieldName}": newValue
          yield return

      @public @async renameField: Function,
        default: (collectionName, oldFieldName, newFieldName)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          yield collection.updateMany {},
            $rename:
              "#{oldFieldName}": newFieldName
          yield return

      @public @async renameIndex: Function,
        default: (collectionName, oldIndexName, newIndexName)->
          # not supported in MongoDB because a index can't been modified
          yield return

      @public @async renameCollection: Function,
        default: (collectionName, newCollectionName)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          yield collection.rename newCollectionName
          yield return

      @public @async dropCollection: Function,
        default: (collectionName)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          if (yield voDB.listCollections(name: collectionName).toArray()).length isnt 0
            yield voDB.dropCollection collectionName
          yield return

      @public @async dropEdgeCollection: Function,
        default: (collectionName1, collectionName2)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collectionName = "#{collectionName1}_#{collectionName2}"
          if (yield voDB.listCollections(name: collectionName).toArray()).length isnt 0
            yield voDB.dropCollection collectionName
          yield return

      @public @async removeField: Function,
        default: (collectionName, fieldName)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          yield collection.updateMany {},
            $unset:
              "#{fieldName}": ''
          , w: 1
          yield return

      @public @async removeIndex: Function,
        default: (collectionName, fieldNames, options)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          indexName = options.name
          unless indexName?
            indexFields = {}
            fieldNames.forEach (fieldName)->
              indexFields[fieldName] = 1
            indexName = yield collection.ensureIndex indexFields,
              unique: options.unique
              sparse: options.sparse
              background: options.background
              name: options.name
          if yield collection.indexExists indexName
            yield collection.dropIndex indexName
          yield return

      @public @async removeTimestamps: Function,
        default: (collectionName, options)->
          {db: dbName} = @collection.getData()
          voDB = yield (yield @collection.connection).db dbName
          collection = yield voDB.collection collectionName
          yield collection.updateMany {},
            $unset:
              createdAt: null
              updatedAt: null
              deletedAt: null
          , w: 1
          yield return


    MongoMigrationMixin.initializeMixin()
