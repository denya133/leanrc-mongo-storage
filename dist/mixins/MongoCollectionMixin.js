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
  var GridFSBucket, MongoClient, Parser,
    hasProp = {}.hasOwnProperty;

  ({MongoClient} = require('mongodb'));

  ({GridFSBucket} = require('mongodb'));

  Parser = require('mongo-parse'); //mongo-parse@2.0.2

  /*
  ```coffee
   * in application when its need

  module.exports = (Module)->
    class MongoCollection extends Module::Collection
      @inheritProtected()
      @include Module::MongoCollectionMixin
      @module Module

      @initialize()
  ```
   */
  module.exports = function(Module) {
    var AnyT, Collection, Cursor, CursorInterface, DEBUG, DictG, EnumG, FuncG, InterfaceG, LEVELS, ListG, MaybeG, Mixin, MomentT, MongoCursor, PointerT, PromiseT, Query, QueryInterface, RecordInterface, SEND_TO_LOG, StreamT, StructG, UnionG, _, _connection, _consumers, assign, co, jsonStringify, moment;
    ({
      AnyT,
      PromiseT,
      StreamT,
      PointerT,
      MomentT,
      FuncG,
      UnionG,
      MaybeG,
      EnumG,
      ListG,
      StructG,
      DictG,
      InterfaceG,
      RecordInterface,
      CursorInterface,
      QueryInterface,
      Collection,
      Query,
      Cursor,
      MongoCursor,
      Mixin,
      LogMessage: {SEND_TO_LOG, LEVELS, DEBUG},
      Utils: {_, co, jsonStringify, moment, assign}
    } = Module.prototype);
    _connection = null;
    _consumers = null;
    return Module.defineMixin(Mixin('MongoCollectionMixin', function(BaseClass = Collection) {
      return (function() {
        var _Class, buildIntervalQuery, ipoBucket, ipoCollection, wrapReference;

        _Class = class extends BaseClass {};

        _Class.inheritProtected();

        ipoCollection = PointerT(_Class.private({
          collection: MaybeG(PromiseT)
        }));

        ipoBucket = PointerT(_Class.private({
          bucket: MaybeG(PromiseT)
        }));

        wrapReference = function(value) {
          if (_.isString(value)) {
            if (/^\@doc\./.test(value)) {
              return value.replace('@doc.', '');
            } else {
              return value.replace('@', '');
            }
          } else {
            return value;
          }
        };

        _Class.public({
          connection: PromiseT
        }, {
          get: function() {
            var self;
            self = this;
            if (_connection == null) {
              _connection = co(function*() {
                var connection, credentials, dbName, db_url, host, mongodb, password, port, ref, username;
                credentials = '';
                mongodb = (ref = self.getData().mongodb) != null ? ref : self.configs.mongodb;
                ({username, password, host, port, dbName} = mongodb);
                if (username && password) {
                  credentials = `${username}:${password}@`;
                }
                db_url = `mongodb://${credentials}${host}:${port}/${dbName}?authSource=admin`;
                connection = (yield MongoClient.connect(db_url));
                return connection;
              });
            }
            return _connection;
          }
        });

        _Class.public({
          collection: PromiseT
        }, {
          get: function() {
            var self;
            self = this;
            if (this[ipoCollection] == null) {
              this[ipoCollection] = co(function*() {
                var connection, name;
                connection = (yield self.connection);
                name = self.collectionFullName();
                return (yield Module.prototype.Promise.new(function(resolve, reject) {
                  connection.collection(name, {
                    strict: true
                  }, function(err, col) {
                    if (err != null) {
                      reject(err);
                    } else {
                      resolve(col);
                    }
                  });
                }));
              });
            }
            return this[ipoCollection];
          }
        });

        _Class.public({
          bucket: PromiseT
        }, {
          get: function() {
            var self;
            self = this;
            if (this[ipoBucket] == null) {
              this[ipoBucket] = co(function*() {
                var connection, dbName, mongodb, ref, voDB;
                mongodb = (ref = self.getData().mongodb) != null ? ref : self.configs.mongodb;
                ({dbName} = mongodb);
                connection = (yield self.connection);
                voDB = connection.db(`${dbName}_fs`);
                return new GridFSBucket(voDB, {
                  chunkSizeBytes: 64512,
                  bucketName: 'binary-store'
                });
              });
            }
            return this[ipoBucket];
          }
        });

        _Class.public({
          onRegister: Function
        }, {
          default: function() {
            this.super();
            (() => {
              return this.connection;
            })();
            if (_consumers == null) {
              _consumers = 0;
            }
            _consumers++;
          }
        });

        _Class.public(_Class.async({
          onRemove: Function
        }, {
          default: function*() {
            var connection;
            this.super();
            _consumers--;
            if (_consumers === 0) {
              connection = (yield _connection);
              yield connection.close(true);
              _connection = void 0;
            }
          }
        }));

        _Class.public(_Class.async({
          push: FuncG(RecordInterface, RecordInterface)
        }, {
          default: function*(aoRecord) {
            var collection, ipoMultitonKey, raw1, snapshot, stats;
            collection = (yield this.collection);
            ipoMultitonKey = this.constructor.instanceVariables['~multitonKey'].pointer;
            stats = (yield collection.stats());
            snapshot = (yield this.serialize(aoRecord));
            raw1 = (yield collection.findOne({
              id: {
                $eq: snapshot.id
              }
            }));
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::push ns = ${stats.ns}, snapshot = ${jsonStringify(snapshot)}`, LEVELS[DEBUG]);
            yield collection.insertOne(snapshot, {
              w: "majority",
              j: true,
              wtimeout: 500
            });
            return (yield this.normalize((yield collection.findOne({
              id: {
                $eq: snapshot.id
              }
            }))));
          }
        }));

        _Class.public(_Class.async({
          remove: FuncG([UnionG(String, Number)])
        }, {
          default: function*(id) {
            var collection, stats;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::remove ns = ${stats.ns}, id = ${id}`, LEVELS[DEBUG]);
            yield collection.deleteOne({
              id: {
                $eq: id
              }
            }, {
              w: "majority",
              j: true,
              wtimeout: 500
            });
          }
        }));

        _Class.public(_Class.async({
          take: FuncG([UnionG(String, Number)], MaybeG(RecordInterface))
        }, {
          default: function*(id) {
            var collection, rawRecord, stats;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::take ns = ${stats.ns}, id = ${id}`, LEVELS[DEBUG]);
            rawRecord = (yield collection.findOne({
              id: {
                $eq: id
              }
            }));
            if (rawRecord != null) {
              return (yield this.normalize(rawRecord));
            } else {

            }
          }
        }));

        _Class.public(_Class.async({
          takeBy: FuncG([Object, MaybeG(Object)], CursorInterface)
        }, {
          default: function*(query, options = {}) {
            var collection, stats, vnLimit, vnOffset, voNativeCursor, voQuery, voSort;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            voQuery = this.parseFilter(Parser.parse(query));
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::takeBy ns = ${stats.ns}, voQuery = ${jsonStringify(voQuery)}`, LEVELS[DEBUG]);
            voNativeCursor = (yield collection.find(voQuery));
            if ((vnLimit = options.$limit) != null) {
              voNativeCursor = voNativeCursor.limit(vnLimit);
            }
            if ((vnOffset = options.$offset) != null) {
              voNativeCursor = voNativeCursor.skip(vnOffset);
            }
            if ((voSort = options.$sort) != null) {
              voNativeCursor = voNativeCursor.sort(voSort.reduce(function(result, item) {
                var asRef, asSortDirect;
                for (asRef in item) {
                  if (!hasProp.call(item, asRef)) continue;
                  asSortDirect = item[asRef];
                  result[wrapReference(asRef)] = asSortDirect === 'ASC' ? 1 : -1;
                }
                return result;
              }, {}));
            }
            return MongoCursor.new(this, voNativeCursor);
          }
        }));

        _Class.public(_Class.async({
          takeMany: FuncG([ListG(UnionG(String, Number))], CursorInterface)
        }, {
          default: function*(ids) {
            var collection, stats, voNativeCursor;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::takeMany ns = ${stats.ns}, ids = ${jsonStringify(ids)}`, LEVELS[DEBUG]);
            voNativeCursor = (yield collection.find({
              id: {
                $in: ids
              }
            }));
            return MongoCursor.new(this, voNativeCursor);
          }
        }));

        _Class.public(_Class.async({
          takeAll: FuncG([], CursorInterface)
        }, {
          default: function*() {
            var collection, stats, voNativeCursor;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::takeAll ns = ${stats.ns}`, LEVELS[DEBUG]);
            voNativeCursor = (yield collection.find());
            return MongoCursor.new(this, voNativeCursor);
          }
        }));

        _Class.public(_Class.async({
          override: FuncG([UnionG(String, Number), RecordInterface], RecordInterface)
        }, {
          default: function*(id, aoRecord) {
            var collection, rawRecord, snapshot, stats;
            collection = (yield this.collection);
            snapshot = (yield this.serialize(aoRecord));
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::override ns = ${stats.ns}, id = ${id}, snapshot = ${jsonStringify(snapshot)}`, LEVELS[DEBUG]);
            yield collection.updateOne({
              id: {
                $eq: id
              }
            }, {
              $set: snapshot
            }, {
              multi: true,
              w: "majority",
              j: true,
              wtimeout: 500
            });
            rawRecord = (yield collection.findOne({
              id: {
                $eq: id
              }
            }));
            return (yield this.normalize(rawRecord));
          }
        }));

        _Class.public(_Class.async({
          includes: FuncG([UnionG(String, Number)], Boolean)
        }, {
          default: function*(id) {
            var collection, stats;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::includes ns = ${stats.ns}, id = ${id}`, LEVELS[DEBUG]);
            return ((yield collection.findOne({
              id: {
                $eq: id
              }
            }))) != null;
          }
        }));

        _Class.public(_Class.async({
          exists: FuncG(Object, Boolean)
        }, {
          default: function*(query) {
            var collection, stats, voQuery;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            voQuery = this.parseFilter(Parser.parse(query));
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::exists ns = ${stats.ns}, voQuery = ${jsonStringify(voQuery)}`, LEVELS[DEBUG]);
            return ((yield collection.count(voQuery))) !== 0;
          }
        }));

        _Class.public(_Class.async({
          length: FuncG([], Number)
        }, {
          default: function*() {
            var collection, stats;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::length ns = ${stats.ns}`, LEVELS[DEBUG]);
            return stats.count;
          }
        }));

        buildIntervalQuery = FuncG([String, MomentT, EnumG('day', 'week', 'month', 'year'), Boolean], Object)(function(aoKey, aoInterval, aoIntervalSize, aoDirect) {
          var voIntervalEnd, voIntervalStart;
          aoInterval = aoInterval.utc();
          voIntervalStart = aoInterval.startOf(aoIntervalSize).toISOString();
          voIntervalEnd = aoInterval.clone().endOf(aoIntervalSize).toISOString();
          if (aoDirect) {
            return {
              $and: [
                {
                  [`${aoKey}`]: {
                    $gte: voIntervalStart
                  },
                  [`${aoKey}`]: {
                    $lt: voIntervalEnd
                  }
                }
              ]
            };
          } else {
            return {
              $not: {
                $and: [
                  {
                    [`${aoKey}`]: {
                      $gte: voIntervalStart
                    },
                    [`${aoKey}`]: {
                      $lt: voIntervalEnd
                    }
                  }
                ]
              }
            };
          }
        });

        // @TODO Нужно добавить описание входных параметров опреторам и соответственно их проверку
        _Class.public({
          operatorsMap: DictG(String, Function)
        }, {
          default: {
            // Logical Query Operators
            $and: function(def) {
              return {
                $and: def
              };
            },
            $or: function(def) {
              return {
                $or: def
              };
            },
            $not: function(def) {
              return {
                $not: def
              };
            },
            $nor: function(def) {
              return {
                $nor: def // not or # !(a||b) === !a && !b
              };
            },
            
            // Comparison Query Operators (aoSecond is NOT sub-query)
            $eq: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $eq: wrapReference(aoSecond) // ==
                }
              };
            },
            $ne: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $ne: wrapReference(aoSecond) // !=
                }
              };
            },
            $lt: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $lt: wrapReference(aoSecond) // <
                }
              };
            },
            $lte: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $lte: wrapReference(aoSecond) // <=
                }
              };
            },
            $gt: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $gt: wrapReference(aoSecond) // >
                }
              };
            },
            $gte: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $gte: wrapReference(aoSecond) // >=
                }
              };
            },
            $in: function(aoFirst, alItems) { // check value present in array
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $in: alItems
                }
              };
            },
            $nin: function(aoFirst, alItems) { // ... not present in array
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $nin: alItems
                }
              };
            },
            // Array Query Operators
            $all: function(aoFirst, alItems) { // contains some values
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $all: alItems
                }
              };
            },
            $elemMatch: function(aoFirst, aoSecond) { // conditions for complex item
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $elemMatch: aoSecond
                }
              };
            },
            $size: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $size: aoSecond
                }
              };
            },
            // Element Query Operators
            $exists: function(aoFirst, aoSecond) { // condition for check present some value in field
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $exists: aoSecond
                }
              };
            },
            $type: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $type: aoSecond
                }
              };
            },
            // Evaluation Query Operators
            $mod: function(aoFirst, aoSecond) {
              return {
                [`${wrapReference(aoFirst)}`]: {
                  $mod: aoSecond
                }
              };
            },
            $regex: function(aoFirst, aoSecond, aoThird) { // value must be string. ckeck it by RegExp.
              var full, params, regExpDefinitions, regexp, value;
              regExpDefinitions = /^\/([\s\S]*)\/(i?m?)$/i.exec(aoSecond);
              if (regExpDefinitions == null) {
                throw new Error("Invalid Regular Expression");
              }
              [full, regexp, params] = regExpDefinitions;
              value = {
                $regex: new RegExp(regexp, params)
              };
              if (aoThird != null) {
                value["$options"] = aoThird;
              }
              return {
                [`${wrapReference(aoFirst)}`]: value
              };
            },
            $text: function() {
              throw new Error('Not supported');
            },
            $where: function() {
              throw new Error('Not supported');
            },
            // Datetime Query Operators
            $td: function(aoFirst, aoSecond) { // this day (today)
              return buildIntervalQuery(wrapReference(aoFirst), moment(), 'day', aoSecond);
            },
            $ld: function(aoFirst, aoSecond) { // last day (yesterday)
              return buildIntervalQuery(wrapReference(aoFirst), moment().subtract(1, 'days'), 'day', aoSecond);
            },
            $tw: function(aoFirst, aoSecond) { // this week
              return buildIntervalQuery(wrapReference(aoFirst), moment(), 'week', aoSecond);
            },
            $lw: function(aoFirst, aoSecond) { // last week
              return buildIntervalQuery(wrapReference(aoFirst), moment().subtract(1, 'weeks'), 'week', aoSecond);
            },
            $tm: function(aoFirst, aoSecond) { // this month
              return buildIntervalQuery(wrapReference(aoFirst), moment(), 'month', aoSecond);
            },
            $lm: function(aoFirst, aoSecond) { // last month
              return buildIntervalQuery(wrapReference(aoFirst), moment().subtract(1, 'months'), 'month', aoSecond);
            },
            $ty: function(aoFirst, aoSecond) { // this year
              return buildIntervalQuery(wrapReference(aoFirst), moment(), 'year', aoSecond);
            },
            $ly: function(aoFirst, aoSecond) { // last year
              return buildIntervalQuery(wrapReference(aoFirst), moment().subtract(1, 'years'), 'year', aoSecond);
            }
          }
        });

        _Class.public({
          parseFilter: FuncG(InterfaceG({
            field: MaybeG(String),
            parts: MaybeG(ListG(Object)),
            operator: MaybeG(String),
            operand: MaybeG(AnyT),
            implicitField: MaybeG(Boolean)
          }), Object)
        }, {
          default: function({field, parts = [], operator, operand, implicitField}) {
            var customFilter, customFilterFunc;
            if ((field != null) && operator !== '$elemMatch' && parts.length === 0) {
              customFilter = this.delegate.customFilters[field];
              if ((customFilterFunc = customFilter != null ? customFilter[operator] : void 0) != null) {
                return customFilterFunc.call(this, operand);
              } else {
                return this.operatorsMap[operator](field, operand);
              }
            } else if ((field != null) && operator === '$elemMatch') {
              return this.operatorsMap[operator](field, parts.reduce((result, part) => {
                var subquery;
                if (implicitField && (part.field == null) && ((part.parts == null) || part.parts.length === 0)) {
                  subquery = this.operatorsMap[part.operator]('temporaryField', part.operand);
                  return Object.assign(result, subquery.temporaryField);
                } else {
                  return Object.assign(result, this.parseFilter(part));
                }
              }, {}));
            } else {
              return this.operatorsMap[operator != null ? operator : '$and'](parts.map(this.parseFilter.bind(this)));
            }
          }
        });

        _Class.public(_Class.async({
          parseQuery: FuncG([UnionG(Object, QueryInterface)], UnionG(Object, String, QueryInterface))
        }, {
          default: function*(aoQuery) {
            var aggPartial, aggUsed, aoValue, asRef, collect, finAggPartial, finAggUsed, into, intoPartial, intoUsed, isCustomReturn, key, projectObj, value, vhObj, vnLimit, vnOffset, voCollect, voFilter, voHaving, voQuery, voReturn, voSort, vsAvg, vsInto, vsMax, vsMin, vsSum;
            if (aoQuery.$join != null) {
              throw new Error('`$join` not available for Mongo queries');
            }
            if (aoQuery.$let != null) {
              throw new Error('`$let` not available for Mongo queries');
            }
            if (aoQuery.$aggregate != null) {
              throw new Error('`$aggregate` not available for Mongo queries');
            }
            voQuery = {};
            aggUsed = aggPartial = intoUsed = intoPartial = finAggUsed = finAggPartial = null;
            isCustomReturn = false;
            if (aoQuery.$remove != null) {
              if (aoQuery.$into != null) {
                voQuery.queryType = 'removeBy';
                if (aoQuery.$forIn != null) {
                  // работа будет только с одной коллекцией, поэтому не учитываем $forIn
                  voQuery.pipeline = [];
                  if ((voFilter = aoQuery.$filter) != null) {
                    voQuery.pipeline.push({
                      $match: this.parseFilter(Parser.parse(voFilter))
                    });
                  }
                  if ((voSort = aoQuery.$sort) != null) {
                    voQuery.pipeline.push({
                      $sort: voSort.reduce(function(result, item) {
                        var asRef, asSortDirect;
                        for (asRef in item) {
                          if (!hasProp.call(item, asRef)) continue;
                          asSortDirect = item[asRef];
                          result[wrapReference(asRef)] = asSortDirect === 'ASC' ? 1 : -1;
                        }
                        return result;
                      }, {})
                    });
                  }
                  if ((vnOffset = aoQuery.$offset) != null) {
                    voQuery.pipeline.push({
                      $skip: vnOffset
                    });
                  }
                  if ((vnLimit = aoQuery.$limit) != null) {
                    voQuery.pipeline.push({
                      $limit: vnLimit
                    });
                  }
                  isCustomReturn = true;
                  voQuery;
                }
              }
            } else if (aoQuery.$patch != null) {
              if (aoQuery.$into != null) {
                voQuery.queryType = 'patchBy';
                if (aoQuery.$forIn != null) {
                  // работа будет только с одной коллекцией, поэтому не учитываем $forIn
                  voQuery.pipeline = [];
                  if ((voFilter = aoQuery.$filter) != null) {
                    voQuery.pipeline.push({
                      $match: this.parseFilter(Parser.parse(voFilter))
                    });
                  }
                  if ((voSort = aoQuery.$sort) != null) {
                    voQuery.pipeline.push({
                      $sort: voSort.reduce(function(result, item) {
                        var asRef, asSortDirect;
                        for (asRef in item) {
                          if (!hasProp.call(item, asRef)) continue;
                          asSortDirect = item[asRef];
                          result[wrapReference(asRef)] = asSortDirect === 'ASC' ? 1 : -1;
                        }
                        return result;
                      }, {})
                    });
                  }
                  if ((vnOffset = aoQuery.$offset) != null) {
                    voQuery.pipeline.push({
                      $skip: vnOffset
                    });
                  }
                  if ((vnLimit = aoQuery.$limit) != null) {
                    voQuery.pipeline.push({
                      $limit: vnLimit
                    });
                  }
                  voQuery.patch = aoQuery.$patch;
                  isCustomReturn = true;
                  voQuery;
                }
              }
            } else if (aoQuery.$forIn != null) {
              voQuery.queryType = 'query';
              voQuery.pipeline = [];
              if ((voFilter = aoQuery.$filter) != null) {
                voQuery.pipeline.push({
                  $match: this.parseFilter(Parser.parse(voFilter))
                });
              }
              if ((voSort = aoQuery.$sort) != null) {
                voQuery.pipeline.push({
                  $sort: voSort.reduce(function(result, item) {
                    var asRef, asSortDirect;
                    for (asRef in item) {
                      if (!hasProp.call(item, asRef)) continue;
                      asSortDirect = item[asRef];
                      result[wrapReference(asRef)] = asSortDirect === 'ASC' ? 1 : -1;
                    }
                    return result;
                  }, {})
                });
              }
              if ((vnOffset = aoQuery.$offset) != null) {
                voQuery.pipeline.push({
                  $skip: vnOffset
                });
              }
              if ((vnLimit = aoQuery.$limit) != null) {
                voQuery.pipeline.push({
                  $limit: vnLimit
                });
              }
              if ((voCollect = aoQuery.$collect) != null) {
                isCustomReturn = true;
                collect = {};
                for (asRef in voCollect) {
                  if (!hasProp.call(voCollect, asRef)) continue;
                  aoValue = voCollect[asRef];
                  ((asRef, aoValue) => {
                    return collect[wrapReference(asRef)] = wrapReference(aoValue);
                  })(asRef, aoValue);
                }
                into = (vsInto = aoQuery.$into) != null ? wrapReference(vsInto) : 'GROUP';
                voQuery.pipeline.push({
                  $group: {
                    _id: collect,
                    [`${into}`]: {
                      $push: Object.keys(this.delegate.attributes).reduce(function(p, c) {
                        p[c] = `$${c}`;
                        return p;
                      }, {})
                    }
                  }
                });
              }
              if ((voHaving = aoQuery.$having) != null) {
                voQuery.pipeline.push({
                  $match: this.parseFilter(Parser.parse(voHaving))
                });
              }
              if (aoQuery.$count != null) {
                isCustomReturn = true;
                voQuery.pipeline.push({
                  $count: 'result'
                });
              } else if ((vsSum = aoQuery.$sum) != null) {
                isCustomReturn = true;
                voQuery.pipeline.push({
                  $group: {
                    _id: null,
                    result: {
                      $sum: `$${wrapReference(vsSum)}`
                    }
                  }
                });
                voQuery.pipeline.push({
                  $project: {
                    _id: 0
                  }
                });
              } else if ((vsMin = aoQuery.$min) != null) {
                isCustomReturn = true;
                voQuery.pipeline.push({
                  $sort: {
                    [`${wrapReference(vsMin)}`]: 1
                  }
                });
                voQuery.pipeline.push({
                  $limit: 1
                });
                voQuery.pipeline.push({
                  $project: {
                    _id: 0,
                    result: `$${wrapReference(vsMin)}`
                  }
                });
              } else if ((vsMax = aoQuery.$max) != null) {
                isCustomReturn = true;
                voQuery.pipeline.push({
                  $sort: {
                    [`${wrapReference(vsMax)}`]: -1
                  }
                });
                voQuery.pipeline.push({
                  $limit: 1
                });
                voQuery.pipeline.push({
                  $project: {
                    _id: 0,
                    result: `$${wrapReference(vsMax)}`
                  }
                });
              } else if ((vsAvg = aoQuery.$avg) != null) {
                isCustomReturn = true;
                voQuery.pipeline.push({
                  $group: {
                    _id: null,
                    result: {
                      $avg: `$${wrapReference(vsAvg)}`
                    }
                  }
                });
                voQuery.pipeline.push({
                  $project: {
                    _id: 0
                  }
                });
              } else {
                if ((voReturn = aoQuery.$return) != null) {
                  if (voReturn !== '@doc') {
                    isCustomReturn = true;
                  }
                  if (_.isString(voReturn)) {
                    if (voReturn !== '@doc') {
                      voQuery.pipeline.push({
                        $project: {
                          _id: 0,
                          [`${wrapReference(voReturn)}`]: 1
                        }
                      });
                    }
                  } else if (_.isObject(voReturn)) {
                    vhObj = {};
                    projectObj = {};
                    for (key in voReturn) {
                      if (!hasProp.call(voReturn, key)) continue;
                      value = voReturn[key];
                      (function(key, value) {
                        vhObj[key] = `$${wrapReference(value)}`;
                        return projectObj[key] = 1;
                      })(key, value);
                    }
                    voQuery.pipeline.push({
                      $addFields: vhObj
                    });
                    voQuery.pipeline.push({
                      $project: projectObj
                    });
                  }
                  if (aoQuery.$distinct) {
                    voQuery.pipeline.push({
                      $group: {
                        _id: '$$CURRENT'
                      }
                    });
                  }
                }
              }
            }
            voQuery.isCustomReturn = isCustomReturn != null ? isCustomReturn : false;
            return voQuery;
          }
        }));

        _Class.public(_Class.async({
          executeQuery: FuncG([UnionG(Object, String, QueryInterface)], CursorInterface)
        }, {
          default: function*(aoQuery, options) {
            var collection, ids, stats, subCursor, voCursor, voNativeCursor, voPipeline;
            collection = (yield this.collection);
            stats = (yield collection.stats());
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::executeQuery ns = ${stats.ns}, aoQuery = ${jsonStringify(aoQuery)}`, LEVELS[DEBUG]);
            voNativeCursor = (yield* (function*() {
              switch (aoQuery.queryType) {
                case 'query':
                  return (yield collection.aggregate(aoQuery.pipeline, {
                    cursor: {
                      batchSize: 1
                    }
                  }));
                case 'patchBy':
                  voPipeline = aoQuery.pipeline;
                  voPipeline.push({
                    $project: {
                      _id: 1
                    }
                  });
                  subCursor = MongoCursor.new(null, (yield collection.aggregate(voPipeline, {
                    cursor: {
                      batchSize: 1000
                    }
                  })));
                  ids = (yield subCursor.map(co.wrap(function*(i) {
                    return i._id;
                  })));
                  yield collection.updateMany({
                    _id: {
                      $in: ids
                    }
                  }, {
                    $set: aoQuery.patch
                  }, {
                    multi: true,
                    w: "majority",
                    j: true,
                    wtimeout: 500
                  });
                  return null;
                case 'removeBy':
                  voPipeline = aoQuery.pipeline;
                  voPipeline.push({
                    $project: {
                      _id: 1
                    }
                  });
                  subCursor = MongoCursor.new(null, (yield collection.aggregate(voPipeline, {
                    cursor: {
                      batchSize: 1000
                    }
                  })));
                  ids = (yield subCursor.map(co.wrap(function*(i) {
                    return i._id;
                  })));
                  yield collection.deleteMany({
                    _id: {
                      $in: ids
                    }
                  }, {
                    w: "majority",
                    j: true,
                    wtimeout: 500
                  });
                  return null;
              }
            })());
            voCursor = aoQuery.isCustomReturn ? voNativeCursor != null ? MongoCursor.new(null, voNativeCursor) : Cursor.new(null, []) : MongoCursor.new(this, voNativeCursor);
            return voCursor;
          }
        }));

        _Class.public(_Class.async({
          createFileWriteStream: FuncG([
            StructG({
              _id: String
            }),
            MaybeG(Object)
          ], StreamT)
        }, {
          default: function*(opts, metadata = {}) {
            var bucket, dbName, mongodb, ref;
            bucket = (yield this.bucket);
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::createFileWriteStream opts = ${jsonStringify(opts)}`, LEVELS[DEBUG]);
            mongodb = (ref = this.getData().mongodb) != null ? ref : this.configs.mongodb;
            ({dbName} = mongodb);
            metadata = assign({}, {dbName}, metadata);
            return bucket.openUploadStream(opts._id, {metadata});
          }
        }));

        _Class.public(_Class.async({
          createFileReadStream: FuncG([
            StructG({
              _id: String
            })
          ], MaybeG(StreamT))
        }, {
          default: function*(opts) {
            var bucket;
            bucket = (yield this.bucket);
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::createFileReadStream opts = ${jsonStringify(opts)}`, LEVELS[DEBUG]);
            if ((yield this.fileExists(opts))) {
              return bucket.openDownloadStreamByName(opts._id, {});
            } else {

            }
          }
        }));

        _Class.public(_Class.async({
          fileExists: FuncG([
            StructG({
              _id: String
            })
          ], Boolean)
        }, {
          default: function*(opts) {
            var bucket;
            bucket = (yield this.bucket);
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::fileExists opts = ${jsonStringify(opts)}`, LEVELS[DEBUG]);
            return (yield ((yield bucket.find({
              filename: opts._id
            }))).hasNext());
          }
        }));

        _Class.public(_Class.async({
          removeFile: FuncG([
            StructG({
              _id: String
            })
          ])
        }, {
          default: function*(opts) {
            var bucket, cursor, file;
            bucket = (yield this.bucket);
            this.sendNotification(SEND_TO_LOG, `MongoCollectionMixin::removeFile opts = ${jsonStringify(opts)}`, LEVELS[DEBUG]);
            cursor = (yield bucket.find({
              filename: opts._id
            }));
            if ((file = (yield cursor.next())) != null) {
              yield bucket.delete(file._id);
            }
          }
        }));

        _Class.initializeMixin();

        return _Class;

      }).call(this);
    }));
  };

}).call(this);
