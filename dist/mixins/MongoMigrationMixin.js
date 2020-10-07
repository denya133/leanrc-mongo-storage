(function() {
  // This file is part of leanrc-mongo-storage.

  // leanrc-mongo-storage is free software: you can redistribute it and/or modify
  // it under the terms of the GNU Lesser General Public License as published by
  // the Free Software Foundation, either version 3 of the License, or
  // (at your option) any later version.

  // leanrc-mongo-storage is distributed in the hope that it will be useful,
  // but WITHOUT ANY WARRANTY; without even the implied warranty of
  // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  // GNU Lesser General Public License for more details.

  // You should have received a copy of the GNU Lesser General Public License
  // along with leanrc-mongo-storage.  If not, see <https://www.gnu.org/licenses/>.
  /*
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
  */
  // Миксин объявляет реализации для виртуальных методов основного Migration класса
  // миксин должен содержать нативный платформозависимый код для обращения к релаьной базе данных на понятном ей языке.
  module.exports = function(Module) {
    var AnyT, AsyncFuncG, DEBUG, EnumG, FuncG, InterfaceG, LEVELS, ListG, MaybeG, Migration, Mixin, SEND_TO_LOG, UnionG, _, co, getCollection, jsonStringify;
    ({
      AnyT,
      FuncG,
      ListG,
      EnumG,
      MaybeG,
      UnionG,
      InterfaceG,
      AsyncFuncG,
      Migration,
      Mixin,
      LogMessage: {SEND_TO_LOG, LEVELS, DEBUG},
      Utils: {_, co, jsonStringify}
    } = Module.prototype);
    getCollection = AsyncFuncG([Object, String], Object)(co.wrap(function*(db, collectionFullName) {
      return (yield Module.prototype.Promise.new(function(resolve, reject) {
        db.collection(collectionFullName, {
          strict: true
        }, function(err, col) {
          if (err != null) {
            reject(err);
          } else {
            resolve(col);
          }
        });
      }));
    }));
    return Module.defineMixin(Mixin('MongoMigrationMixin', function(BaseClass = Migration) {
      return (function() {
        var DOWN, SUPPORTED_TYPES, UP, _Class;

        _Class = class extends BaseClass {};

        _Class.inheritProtected();

        ({UP, DOWN, SUPPORTED_TYPES} = _Class.prototype);

        _Class.public(_Class.async({
          createCollection: FuncG([String, MaybeG(Object)])
        }, {
          default: function*(collectionName, options = {}) {
            var qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::createCollection qualifiedName = ${qualifiedName}, options = ${jsonStringify(options)}`, LEVELS[DEBUG]);
            yield voDB.createCollection(qualifiedName, options);
          }
        }));

        _Class.public(_Class.async({
          createEdgeCollection: FuncG([String, String, MaybeG(Object)])
        }, {
          default: function*(collectionName1, collectionName2, options = {}) {
            var qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(`${collectionName1}_${collectionName2}`);
            voDB = (yield this.collection.connection);
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::createEdgeCollection qualifiedName = ${qualifiedName}, options = ${jsonStringify(options)}`, LEVELS[DEBUG]);
            yield voDB.createCollection(qualifiedName, options);
          }
        }));

        _Class.public(_Class.async({
          addField: FuncG([
            String,
            String,
            UnionG(EnumG(SUPPORTED_TYPES),
            InterfaceG({
              type: EnumG(SUPPORTED_TYPES),
              default: AnyT
            }))
          ])
        }, {
          default: function*(collectionName, fieldName, options) {
            var collection, initial, qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            if (_.isString(options)) {
              return;
            }
            if (options.default != null) {
              if (_.isNumber(options.default) || _.isBoolean(options.default)) {
                initial = options.default;
              } else if (_.isDate(options.default)) {
                initial = options.default.toISOString();
              } else if (_.isString(options.default)) {
                initial = `${options.default}`;
              } else {
                initial = null;
              }
            } else {
              initial = null;
            }
            if (initial != null) {
              voDB = (yield this.collection.connection);
              this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::addField qualifiedName = ${qualifiedName}, $set: ${jsonStringify({
                [`${fieldName}`]: initial
              })}`, LEVELS[DEBUG]);
              collection = (yield getCollection(voDB, qualifiedName));
              yield collection.updateMany({}, {
                $set: {
                  [`${fieldName}`]: initial
                }
              }, {
                w: 1
              });
            }
          }
        }));

        _Class.public(_Class.async({
          addIndex: FuncG([
            String,
            ListG(String),
            InterfaceG({
              type: EnumG('hash',
            'skiplist',
            'persistent',
            'geo',
            'fulltext'),
              unique: MaybeG(Boolean),
              sparse: MaybeG(Boolean)
            })
          ])
        }, {
          default: function*(collectionName, fieldNames, options) {
            var collection, indexFields, opts, qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            indexFields = {};
            fieldNames.forEach(function(fieldName) {
              return indexFields[fieldName] = 1;
            });
            opts = {
              unique: options.unique,
              sparse: options.sparse,
              background: options.background,
              name: options.name
            };
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::addIndex indexFields = ${jsonStringify(indexFields)}, opts = ${jsonStringify(opts)}`, LEVELS[DEBUG]);
            yield collection.ensureIndex(indexFields, opts);
          }
        }));

        _Class.public(_Class.async({
          addTimestamps: FuncG([String, MaybeG(Object)])
        }, {
          default: function*(collectionName, options = {}) {}
        }));

        // NOTE: нет смысла выполнять запрос, т.к. в addField есть проверка if initial? и если null, то атрибут не добавляется
        _Class.public(_Class.async({
          changeCollection: FuncG([String, Object])
        }, {
          default: function*(name, options) {}
        }));

        // not supported in MongoDB because a collection can't been modified
        _Class.public(_Class.async({
          changeField: FuncG([
            String,
            String,
            UnionG(EnumG(SUPPORTED_TYPES),
            InterfaceG({
              type: EnumG(SUPPORTED_TYPES)
            }))
          ])
        }, {
          default: function*(collectionName, fieldName, options) {
            var array, binary, boolean, collection, cursor, date, datetime, decimal, document, float, hash, integer, json, newValue, number, primary_key, qualifiedName, string, text, time, timestamp, type, voDB;
            ({json, binary, boolean, date, datetime, number, decimal, float, integer, primary_key, string, text, time, timestamp, array, hash} = SUPPORTED_TYPES);
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            cursor = (yield collection.find().batchSize(1));
            type = _.isString(options) ? options : options.type;
            while ((yield cursor.hasNext())) {
              document = (yield cursor.next());
              newValue = (function() {
                switch (type) {
                  case boolean:
                    return Boolean(document[fieldName]);
                  case decimal:
                  case float:
                  case integer:
                  case number:
                    return Number(document[fieldName]);
                  case string:
                  case text:
                  case primary_key:
                  case binary:
                  case array:
                    return JSON.stringify(document[fieldName]);
                  case json:
                  case hash:
                    return JSON.parse(String(document[fieldName]));
                  case date:
                  case datetime:
                    return (new Date(document[fieldName])).toISOString();
                  case time:
                  case timestamp:
                    return Number(new Date(document[fieldName]));
                }
              })();
              this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::changeField qualifiedName = ${qualifiedName}, _id: ${jsonStringify(document._id)}, $set: ${jsonStringify({
                [`${fieldName}`]: newValue
              })}`, LEVELS[DEBUG]);
              yield collection.updateOne({
                _id: document._id
              }, {
                $set: {
                  [`${fieldName}`]: newValue
                }
              });
            }
          }
        }));

        _Class.public(_Class.async({
          renameField: FuncG([String, String, String])
        }, {
          default: function*(collectionName, oldFieldName, newFieldName) {
            var collection, qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::renameField qualifiedName = ${qualifiedName}, $rename: ${jsonStringify({
              [`${oldFieldName}`]: newFieldName
            })}`, LEVELS[DEBUG]);
            yield collection.updateMany({}, {
              $rename: {
                [`${oldFieldName}`]: newFieldName
              }
            });
          }
        }));

        _Class.public(_Class.async({
          renameIndex: FuncG([String, String, String])
        }, {
          default: function*(collectionName, oldIndexName, newIndexName) {}
        }));

        // not supported in MongoDB because a index can't been modified
        _Class.public(_Class.async({
          renameCollection: FuncG([String, String])
        }, {
          default: function*(collectionName, newCollectionName) {
            var collection, newQualifiedName, qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            newQualifiedName = this.collection.collectionFullName(newCollectionName);
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::renameCollection qualifiedName = ${qualifiedName}, newQualifiedName = ${newQualifiedName}`, LEVELS[DEBUG]);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            yield collection.rename(newQualifiedName);
          }
        }));

        _Class.public(_Class.async({
          dropCollection: FuncG(String)
        }, {
          default: function*(collectionName) {
            var qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            if (((yield voDB.listCollections({
              name: qualifiedName
            }).toArray())).length !== 0) {
              this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::dropCollection qualifiedName = ${qualifiedName}`, LEVELS[DEBUG]);
              yield voDB.dropCollection(qualifiedName);
            }
          }
        }));

        _Class.public(_Class.async({
          dropEdgeCollection: FuncG([String, String])
        }, {
          default: function*(collectionName1, collectionName2) {
            var qualifiedName, voDB;
            voDB = (yield this.collection.connection);
            qualifiedName = this.collection.collectionFullName(`${collectionName1}_${collectionName2}`);
            if (((yield voDB.listCollections({
              name: qualifiedName
            }).toArray())).length !== 0) {
              this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::dropEdgeCollection qualifiedName = ${qualifiedName}`, LEVELS[DEBUG]);
              yield voDB.dropCollection(qualifiedName);
            }
          }
        }));

        _Class.public(_Class.async({
          removeField: FuncG([String, String])
        }, {
          default: function*(collectionName, fieldName) {
            var collection, qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::removeField qualifiedName = ${qualifiedName}, $unset: ${jsonStringify({
              [`${fieldName}`]: ''
            })}`, LEVELS[DEBUG]);
            yield collection.updateMany({}, {
              $unset: {
                [`${fieldName}`]: ''
              }
            }, {
              w: 1
            });
          }
        }));

        _Class.public(_Class.async({
          removeIndex: FuncG([
            String,
            ListG(String),
            InterfaceG({
              type: EnumG('hash',
            'skiplist',
            'persistent',
            'geo',
            'fulltext'),
              unique: MaybeG(Boolean),
              sparse: MaybeG(Boolean)
            })
          ])
        }, {
          default: function*(collectionName, fieldNames, options) {
            var collection, indexFields, indexName, qualifiedName, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            indexName = options.name;
            if (indexName == null) {
              indexFields = {};
              fieldNames.forEach(function(fieldName) {
                return indexFields[fieldName] = 1;
              });
              indexName = (yield collection.ensureIndex(indexFields, {
                unique: options.unique,
                sparse: options.sparse,
                background: options.background,
                name: options.name
              }));
            }
            if ((yield collection.indexExists(indexName))) {
              this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::removeIndex qualifiedName = ${qualifiedName}, indexName = ${indexName}, indexFields = ${jsonStringify(indexFields)}, options = ${jsonStringify(options)}`, LEVELS[DEBUG]);
              yield collection.dropIndex(indexName);
            }
          }
        }));

        _Class.public(_Class.async({
          removeTimestamps: FuncG([String, MaybeG(Object)])
        }, {
          default: function*(collectionName, options = {}) {
            var collection, qualifiedName, timestamps, voDB;
            qualifiedName = this.collection.collectionFullName(collectionName);
            voDB = (yield this.collection.connection);
            collection = (yield getCollection(voDB, qualifiedName));
            timestamps = {
              createdAt: null,
              updatedAt: null,
              deletedAt: null
            };
            this.collection.sendNotification(SEND_TO_LOG, `MongoMigrationMixin::removeTimestamps qualifiedName = ${qualifiedName}, $unset: ${jsonStringify(timestamps)}`, LEVELS[DEBUG]);
            yield collection.updateMany({}, {
              $unset: timestamps
            }, {
              w: 1
            });
          }
        }));

        _Class.initializeMixin();

        return _Class;

      }).call(this);
    }));
  };

}).call(this);
