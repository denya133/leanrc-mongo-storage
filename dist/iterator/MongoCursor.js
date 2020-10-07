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
  module.exports = function(Module) {
    var AnyT, CollectionInterface, CoreObject, CursorInterface, FuncG, MaybeG, MongoCursor, PointerT, UnionG, _;
    ({
      AnyT,
      PointerT,
      FuncG,
      MaybeG,
      UnionG,
      CollectionInterface,
      CursorInterface,
      CoreObject,
      Utils: {_}
    } = Module.prototype);
    return MongoCursor = (function() {
      var ipoCollection, ipoCursor;

      class MongoCursor extends CoreObject {};

      MongoCursor.inheritProtected();

      MongoCursor.implements(CursorInterface);

      MongoCursor.module(Module);

      ipoCursor = PointerT(MongoCursor.private({
        cursor: MaybeG(Object)
      }));

      ipoCollection = PointerT(MongoCursor.private({
        collection: MaybeG(CollectionInterface)
      }));

      MongoCursor.public({
        isClosed: Boolean
      }, {
        get: function() {
          var ref, ref1;
          return (ref = (ref1 = this[ipoCursor]) != null ? ref1.isClosed() : void 0) != null ? ref : true;
        }
      });

      MongoCursor.public({
        setIterable: FuncG(Object, CursorInterface)
      }, {
        default: function(aoCursor) {
          this[ipoCursor] = aoCursor;
          return this;
        }
      });

      MongoCursor.public({
        setCollection: FuncG(CollectionInterface, CursorInterface)
      }, {
        default: function(aoCollection) {
          this[ipoCollection] = aoCollection;
          return this;
        }
      });

      MongoCursor.public(MongoCursor.async({
        toArray: FuncG([], Array)
      }, {
        default: function*() {
          var results;
          results = [];
          while ((yield this.hasNext())) {
            results.push((yield this.next()));
          }
          return results;
        }
      }));

      MongoCursor.public(MongoCursor.async({
        next: FuncG([], MaybeG(AnyT))
      }, {
        default: function*() {
          var data, ref;
          if (this[ipoCursor] == null) {
            return;
          }
          data = (yield this[ipoCursor].next());
          switch (false) {
            case !(data == null):
              return data;
            case this[ipoCollection] == null:
              return (yield ((ref = this[ipoCollection]) != null ? ref.normalize(data) : void 0));
            default:
              return data;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        hasNext: FuncG([], Boolean)
      }, {
        default: function*() {
          return (yield !this.isClosed && ((yield this[ipoCursor].hasNext())));
        }
      }));

      MongoCursor.public(MongoCursor.async({
        close: Function
      }, {
        default: function*() {
          var ref;
          yield Module.prototype.Promise.resolve((ref = this[ipoCursor]) != null ? ref.close() : void 0);
        }
      }));

      MongoCursor.public(MongoCursor.async({
        count: FuncG([], Number)
      }, {
        default: function*() {
          if (this[ipoCursor] == null) {
            return 0;
          }
          return (yield (yield this[ipoCursor].count(true)));
        }
      }));

      MongoCursor.public(MongoCursor.async({
        forEach: FuncG(Function)
      }, {
        default: function*(lambda) {
          var err, index;
          index = 0;
          try {
            while ((yield this.hasNext())) {
              yield lambda((yield this.next()), index++);
            }
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        map: FuncG(Function, Array)
      }, {
        default: function*(lambda) {
          var err, index, results;
          index = 0;
          try {
            results = [];
            while ((yield this.hasNext())) {
              results.push((yield lambda((yield this.next()), index++)));
            }
            return results;
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        filter: FuncG(Function, Array)
      }, {
        default: function*(lambda) {
          var err, index, record, records;
          index = 0;
          records = [];
          try {
            while ((yield this.hasNext())) {
              record = (yield this.next());
              if ((yield lambda(record, index++))) {
                records.push(record);
              }
            }
            return records;
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        find: FuncG(Function, MaybeG(AnyT))
      }, {
        default: function*(lambda) {
          var _record, err, index, record;
          index = 0;
          _record = null;
          try {
            while ((yield this.hasNext())) {
              record = (yield this.next());
              if ((yield lambda(record, index++))) {
                _record = record;
                break;
              }
            }
            return _record;
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        compact: FuncG([], Array)
      }, {
        default: function*() {
          var err, rawRecord, record, records;
          if (this[ipoCursor] == null) {
            return [];
          }
          records = [];
          try {
            while ((yield this.hasNext())) {
              rawRecord = (yield this[ipoCursor].next());
              if (!_.isEmpty(rawRecord)) {
                record = this[ipoCollection] != null ? (yield this[ipoCollection].normalize(rawResult)) : rawResult;
                records.push(record);
              }
            }
            return records;
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        reduce: FuncG([Function, AnyT], AnyT)
      }, {
        default: function*(lambda, initialValue) {
          var _initialValue, err, index;
          try {
            index = 0;
            _initialValue = initialValue;
            while ((yield this.hasNext())) {
              _initialValue = (yield lambda(_initialValue, (yield this.next()), index++));
            }
            return _initialValue;
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.async({
        first: FuncG([], MaybeG(AnyT))
      }, {
        default: function*() {
          var err, result;
          try {
            result = (yield this.hasNext()) ? (yield this.next()) : null;
            yield this.close();
            return result;
          } catch (error) {
            err = error;
            yield this.close();
            throw err;
          }
        }
      }));

      MongoCursor.public(MongoCursor.static(MongoCursor.async({
        restoreObject: Function
      }, {
        default: function*() {
          throw new Error(`restoreObject method not supported for ${this.name}`);
        }
      })));

      MongoCursor.public(MongoCursor.static(MongoCursor.async({
        replicateObject: Function
      }, {
        default: function*() {
          throw new Error(`replicateObject method not supported for ${this.name}`);
        }
      })));

      MongoCursor.public({
        init: FuncG([MaybeG(CollectionInterface), MaybeG(Object)])
      }, {
        default: function(aoCollection = null, aoCursor = null) {
          this.super(...arguments);
          if (aoCollection != null) {
            this[ipoCollection] = aoCollection;
          }
          if (aoCursor != null) {
            this[ipoCursor] = aoCursor;
          }
        }
      });

      MongoCursor.initialize();

      return MongoCursor;

    }).call(this);
  };

}).call(this);
