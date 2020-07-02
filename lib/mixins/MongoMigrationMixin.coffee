# This file is part of leanrc-mongo-storage.
#
# leanrc-mongo-storage is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# leanrc-mongo-storage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with leanrc-mongo-storage.  If not, see <https://www.gnu.org/licenses/>.

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
    AnyT
    FuncG, ListG, EnumG, MaybeG, UnionG, InterfaceG, AsyncFuncG
    Migration
    Mixin
    LogMessage: {
      SEND_TO_LOG
      LEVELS
      DEBUG
    }
    Utils: { _, co, jsonStringify }
  } = Module::

  getCollection = AsyncFuncG(
    [Object, String], Object
  ) co.wrap (db, collectionFullName) ->
    return yield Module::Promise.new (resolve, reject) ->
      db.collection collectionFullName, strict: yes, (err, col) ->
        if err? then reject err else resolve col
        return
      return

  Module.defineMixin Mixin 'MongoMigrationMixin', (BaseClass = Migration) ->
    class extends BaseClass
      @inheritProtected()

      { UP, DOWN, SUPPORTED_TYPES } = @::

      @public @async createCollection: FuncG([String, MaybeG Object]),
        default: (collectionName, options = {})->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::createCollection qualifiedName = #{qualifiedName}, options = #{jsonStringify options}", LEVELS[DEBUG])
          yield voDB.createCollection qualifiedName, options
          yield return

      @public @async createEdgeCollection: FuncG([String, String, MaybeG Object]),
        default: (collectionName1, collectionName2, options = {})->
          qualifiedName = @collection.collectionFullName "#{collectionName1}_#{collectionName2}"
          voDB = yield @collection.connection
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::createEdgeCollection qualifiedName = #{qualifiedName}, options = #{jsonStringify options}", LEVELS[DEBUG])
          yield voDB.createCollection qualifiedName, options
          yield return

      @public @async addField: FuncG([String, String, UnionG(
        EnumG SUPPORTED_TYPES
        InterfaceG {
          type: EnumG SUPPORTED_TYPES
          default: AnyT
        }
      )]),
        default: (collectionName, fieldName, options)->
          qualifiedName = @collection.collectionFullName collectionName
          if _.isString options
            yield return
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
          if initial?
            voDB = yield @collection.connection
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::addField qualifiedName = #{qualifiedName}, $set: #{jsonStringify "#{fieldName}": initial}", LEVELS[DEBUG])
            collection = yield getCollection voDB, qualifiedName
            yield collection.updateMany {},
              $set:
                "#{fieldName}": initial
            , w: 1
          yield return

      @public @async addIndex: FuncG([String, ListG(String), InterfaceG {
        type: EnumG 'hash', 'skiplist', 'persistent', 'geo', 'fulltext'
        unique: MaybeG Boolean
        sparse: MaybeG Boolean
      }]),
        default: (collectionName, fieldNames, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
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

      @public @async addTimestamps: FuncG([String, MaybeG Object]),
        default: (collectionName, options = {})->
          # NOTE: нет смысла выполнять запрос, т.к. в addField есть проверка if initial? и если null, то атрибут не добавляется
          yield return

      @public @async changeCollection: FuncG([String, Object]),
        default: (name, options)->
          # not supported in MongoDB because a collection can't been modified
          yield return

      @public @async changeField: FuncG([String, String, UnionG(
        EnumG SUPPORTED_TYPES
        InterfaceG {
          type: EnumG SUPPORTED_TYPES
        }
      )]),
        default: (collectionName, fieldName, options)->
          {
            json
            binary
            boolean
            date
            datetime
            number
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
          } = SUPPORTED_TYPES
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
          cursor = yield collection.find().batchSize(1)
          type = if _.isString options
            options
          else
            options.type
          while yield cursor.hasNext()
            document = yield cursor.next()
            newValue = switch type
              when boolean
                Boolean document[fieldName]
              when decimal, float, integer, number
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

      @public @async renameField: FuncG([String, String, String]),
        default: (collectionName, oldFieldName, newFieldName)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::renameField qualifiedName = #{qualifiedName}, $rename: #{jsonStringify "#{oldFieldName}": newFieldName}", LEVELS[DEBUG])
          yield collection.updateMany {},
            $rename:
              "#{oldFieldName}": newFieldName
          yield return

      @public @async renameIndex: FuncG([String, String, String]),
        default: (collectionName, oldIndexName, newIndexName)->
          # not supported in MongoDB because a index can't been modified
          yield return

      @public @async renameCollection: FuncG([String, String]),
        default: (collectionName, newCollectionName)->
          qualifiedName = @collection.collectionFullName collectionName
          newQualifiedName = @collection.collectionFullName newCollectionName
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::renameCollection qualifiedName = #{qualifiedName}, newQualifiedName = #{newQualifiedName}", LEVELS[DEBUG])
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
          yield collection.rename newQualifiedName
          yield return

      @public @async dropCollection: FuncG(String),
        default: (collectionName)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          if (yield voDB.listCollections(name: qualifiedName).toArray()).length isnt 0
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::dropCollection qualifiedName = #{qualifiedName}", LEVELS[DEBUG])
            yield voDB.dropCollection qualifiedName
          yield return

      @public @async dropEdgeCollection: FuncG([String, String]),
        default: (collectionName1, collectionName2)->
          voDB = yield @collection.connection
          qualifiedName = @collection.collectionFullName "#{collectionName1}_#{collectionName2}"
          if (yield voDB.listCollections(name: qualifiedName).toArray()).length isnt 0
            @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::dropEdgeCollection qualifiedName = #{qualifiedName}", LEVELS[DEBUG])
            yield voDB.dropCollection qualifiedName
          yield return

      @public @async removeField: FuncG([String, String]),
        default: (collectionName, fieldName)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
          @collection.sendNotification(SEND_TO_LOG, "MongoMigrationMixin::removeField qualifiedName = #{qualifiedName}, $unset: #{jsonStringify "#{fieldName}": ''}", LEVELS[DEBUG])
          yield collection.updateMany {},
            $unset:
              "#{fieldName}": ''
          , w: 1
          yield return

      @public @async removeIndex: FuncG([String, ListG(String), InterfaceG {
        type: EnumG 'hash', 'skiplist', 'persistent', 'geo', 'fulltext'
        unique: MaybeG Boolean
        sparse: MaybeG Boolean
      }]),
        default: (collectionName, fieldNames, options)->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
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

      @public @async removeTimestamps: FuncG([String, MaybeG Object]),
        default: (collectionName, options = {})->
          qualifiedName = @collection.collectionFullName collectionName
          voDB = yield @collection.connection
          collection = yield getCollection voDB, qualifiedName
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
