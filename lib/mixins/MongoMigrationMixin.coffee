

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
  Module.defineMixin (BaseClass) ->
    class MongoMigrationMixin extends BaseClass
      @inheritProtected()

      @public @async createCollection: Function,
        default: (name, options)->
          qualifiedName = @collection.collectionFullName name
          unless db._collection qualifiedName
            db._createDocumentCollection qualifiedName, options
          yield return

      @public @async createEdgeCollection: Function,
        default: (collection_1, collection_2, options)->
          qualifiedName = @collection.collectionFullName "#{collection_1}_#{collection_2}"
          unless db._collection qualifiedName
            db._createEdgeCollection qualifiedName, options
          yield return

      @public @async addField: Function,
        default: (collection_name, field_name, options)->
          qualifiedName = @collection.collectionFullName collection_name
          if options.default?
            if _.isNumber(options.default) or _.isBoolean(options.default)
              initial = options.default
            else if _.isDate options.default
              initial = options.default.toISOString()
            else if _.isString options.default
              initial = "'#{options.default}'"
            else
              initial = 'null'
          else
            initial = 'null'
          db._query "
            FOR doc IN #{qualifiedName}
              UPDATE doc._key WITH {#{field_name}: #{initial}} IN #{qualifiedName}
          "
          yield return

      @public @async addIndex: Function,
        default: (collection_name, field_names, options)->
          qualifiedName = @collection.collectionFullName collection_name
          db._collection(qualifiedName).ensureIndex
            type: options.type
            fields: field_names
            unique: options.unique
            sparse: options.sparse
          yield return

      @public @async addTimestamps: Function,
        default: (collection_name, options)->
          qualifiedName = @collection.collectionFullName collection_name
          db._query "
            FOR doc IN #{qualifiedName}
              UPDATE doc._key
                WITH {createdAt: null, updatedAt: null, deletedAt: null}
              IN #{qualifiedName}
          "
          yield return

      @public @async changeCollection: Function,
        default: (name, options)->
          qualifiedName = @collection.collectionFullName collection_name
          db._collection(qualifiedName).properties options
          yield return

      @public @async changeField: Function,
        default: (collection_name, field_name, options)->
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
          typeCast = switch options.type
            when boolean
              "TO_BOOL(doc.#{field_name})"
            when decimal, float, integer
              "TO_NUMBER(doc.#{field_name})"
            when string, text, primary_key, binary
              "TO_STRING(JSON_STRINGIFY(doc.#{field_name}))"
            when array
              "TO_ARRAY(doc.#{field_name})"
            when json, hash
              "JSON_PARSE(TO_STRING(doc.#{field_name}))"
            when date, datetime
              "DATE_ISO8601(doc.#{field_name})"
            when time, timestamp
              "DATE_TIMESTAMP(doc.#{field_name})"
          qualifiedName = @collection.collectionFullName collection_name
          db._query "
            FOR doc IN #{qualifiedName}
              UPDATE doc._key
                WITH {#{field_name}: #{typeCast}}
              IN #{qualifiedName}
          "
          yield return

      @public @async renameField: Function,
        default: (collection_name, field_name, new_field_name)->
          qualifiedName = @collection.collectionFullName collection_name
          db._query "
            FOR doc IN #{qualifiedName}
              LET doc_with_n_field = MERGE(doc, {#{new_field_name}: doc.#{field_name}})
              LET doc_without_o_field = UNSET(doc_with_new_field, '#{field_name}')
              REPLACE doc._key
                WITH doc_without_o_field
              IN #{qualifiedName}
          "
          yield return

      @public @async renameIndex: Function,
        default: (collection_name, old_name, new_name)->
          # not supported in ArangoDB because index has not name
          yield return

      @public @async renameCollection: Function,
        default: (collection_name, old_name, new_name)->
          qualifiedName = @collection.collectionFullName collection_name
          newQualifiedName = @collection.collectionFullName new_name
          db._collection(qualifiedName).rename newQualifiedName
          yield return

      @public @async dropCollection: Function,
        default: (name)->
          qualifiedName = @collection.collectionFullName name
          unless db._collection qualifiedName
            db._drop qualifiedName
          yield return

      @public @async dropEdgeCollection: Function,
        default: (collection_1, collection_2)->
          qualifiedName = @collection.collectionFullName "#{collection_1}_#{collection_2}"
          unless db._collection qualifiedName
            db._drop qualifiedName
          yield return

      @public @async removeField: Function,
        default: (collection_name, field_name)->
          qualifiedName = @collection.collectionFullName collection_name
          db._query "
            FOR doc IN #{qualifiedName}
              LET doc_without_f = UNSET(doc, '#{field_name}')
              REPLACE doc._key WITH doc_without_f IN #{qualifiedName}
          "
          yield return

      @public @async removeIndex: Function,
        default: (collection_name, field_names, options)->
          qualifiedName = @collection.collectionFullName collection_name
          index = db._collection(qualifiedName).ensureIndex
            type: options.type
            fields: field_names
            unique: options.unique
            sparse: options.sparse
          db._collection(qualifiedName).dropIndex index
          yield return

      @public @async removeTimestamps: Function,
        default: (collection_name, options)->
          qualifiedName = @collection.collectionFullName collection_name
          db._query "
            FOR doc IN #{qualifiedName}
              LET new_doc = UNSET(doc, 'createdAt', 'updatedAt', 'deletedAt')
              REPLACE doc._key WITH new_doc IN #{qualifiedName}
          "
          yield return


    MongoMigrationMixin.initializeMixin()
