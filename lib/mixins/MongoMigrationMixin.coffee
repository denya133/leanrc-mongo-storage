

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
  {
    Migration
    LogMessage: {
      SEND_TO_LOG
      LEVELS
      DEBUG
    }
    Utils: { _, jsonStringify }
  } = Module::

  Module.defineMixin 'MongoMigrationMixin', (BaseClass = Migration) ->
    class extends BaseClass
      @inheritProtected()

      @public @async createCollection: Function,
        default: (collectionName, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::createCollection qualifiedName = #{qualifiedName}, options = #{jsonStringify options}", LEVELS[DEBUG])
          yield voDB.createCollection qualifiedName, options
          yield return

      @public @async createEdgeCollection: Function,
        default: (collectionName1, collectionName2, options)->
          qualifiedName = @collection.collectionFullName "#{collectionName1}_#{collectionName2}"
          voDB = yield @collection.connection
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::createEdgeCollection qualifiedName = #{qualifiedName}, options = #{jsonStringify options}", LEVELS[DEBUG])
          yield voDB.createCollection qualifiedName, options
          yield return

      @public @async addField: Function,
        default: (collectionName, fieldName, options = {})->
          qualifiedName = @collection.collectionFullName collectionName
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
          voDB = yield @collection.connection
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::addField qualifiedName = #{qualifiedName}, $set: #{jsonStringify "#{fieldName}": initial}", LEVELS[DEBUG])
          collection = yield voDB.collection qualifiedName
          yield collection.updateMany {},
            $set:
              "#{fieldName}": initial
          , w: 1
          yield return

      @public @async addIndex: Function,
        default: (collectionName, fieldNames, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
          indexFields = {}
          fieldNames.forEach (fieldName)->
            indexFields[fieldName] = 1
          opts =
            unique: options.unique
            sparse: options.sparse
            background: options.background
            name: options.name
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::addIndex indexFields = #{jsonStringify indexFields}, opts = #{jsonStringify opts}", LEVELS[DEBUG])
          yield collection.ensureIndex indexFields, opts
          yield return

      @public @async addTimestamps: Function,
        default: (collectionName, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
          timestamps =
            createdAt: null
            updatedAt: null
            deletedAt: null
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::addTimestamps qualifiedName = #{qualifiedName}, $set: #{jsonStringify timestamps}", LEVELS[DEBUG])
          yield collection.updateMany {},
            $set: timestamps
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
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
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
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::changeField qualifiedName = #{qualifiedName}, _id: #{jsonStringify document._id}, $set: #{jsonStringify "#{fieldName}": newValue}", LEVELS[DEBUG])
            yield collection.updateOne
              _id: document._id
            ,
              $set: "#{fieldName}": newValue
          yield return

      @public @async renameField: Function,
        default: (collectionName, oldFieldName, newFieldName)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::renameField qualifiedName = #{qualifiedName}, $rename: #{jsonStringify "#{oldFieldName}": newFieldName}", LEVELS[DEBUG])
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
          qualifiedName = @collection.collectionFullName collectionName
          newQualifiedName = @collection.collectionFullName newCollectionName
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::renameCollection qualifiedName = #{qualifiedName}, newQualifiedName = #{newQualifiedName}", LEVELS[DEBUG])
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
          yield collection.rename newQualifiedName
          yield return

      @public @async dropCollection: Function,
        default: (collectionName)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          if (yield voDB.listCollections(name: qualifiedName).toArray()).length isnt 0
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::dropCollection qualifiedName = #{qualifiedName}", LEVELS[DEBUG])
            yield voDB.dropCollection qualifiedName
          yield return

      @public @async dropEdgeCollection: Function,
        default: (collectionName1, collectionName2)->
          voDB = yield @collection.connection
          qualifiedName = @collection.collectionFullName "#{collectionName1}_#{collectionName2}"
          if (yield voDB.listCollections(name: qualifiedName).toArray()).length isnt 0
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::dropEdgeCollection qualifiedName = #{qualifiedName}", LEVELS[DEBUG])
            yield voDB.dropCollection qualifiedName
          yield return

      @public @async removeField: Function,
        default: (collectionName, fieldName)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::removeField qualifiedName = #{qualifiedName}, $unset: #{jsonStringify "#{fieldName}": ''}", LEVELS[DEBUG])
          yield collection.updateMany {},
            $unset:
              "#{fieldName}": ''
          , w: 1
          yield return

      @public @async removeIndex: Function,
        default: (collectionName, fieldNames, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
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
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::removeIndex qualifiedName = #{qualifiedName}, indexName = #{indexName}, indexFields = #{jsonStringify indexFields}, options = #{jsonStringify options}", LEVELS[DEBUG])
            yield collection.dropIndex indexName
          yield return

      @public @async removeTimestamps: Function,
        default: (collectionName, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield voDB.collection qualifiedName
          timestamps =
            createdAt: null
            updatedAt: null
            deletedAt: null
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::removeTimestamps qualifiedName = #{qualifiedName}, $unset: #{jsonStringify timestamps}", LEVELS[DEBUG])
          yield collection.updateMany {},
            $unset: timestamps
          , w: 1
          yield return


      @initializeMixin()
