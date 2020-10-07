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
  var crypto;

  crypto = require('crypto');

  module.exports = function(Module) {
    var AnyT, FuncG, MaybeG, Mixin, RecordInterface, Serializer, SubsetG;
    ({AnyT, FuncG, SubsetG, MaybeG, RecordInterface, Mixin, Serializer} = Module.prototype);
    return Module.defineMixin(Mixin('MongoSerializerMixin', function(BaseClass = Serializer) {
      return (function() {
        var _Class;

        _Class = class extends BaseClass {};

        _Class.inheritProtected();

        _Class.public(_Class.async({
          normalize: FuncG([SubsetG(RecordInterface), MaybeG(AnyT)], RecordInterface)
        }, {
          default: function*(acRecord, ahPayload) {
            ahPayload.rev = ahPayload._rev;
            ahPayload._rev = void 0;
            delete ahPayload._rev;
            return (yield acRecord.normalize(ahPayload, this.collection));
          }
        }));

        _Class.public(_Class.async({
          serialize: FuncG([MaybeG(RecordInterface), MaybeG(Object)], MaybeG(AnyT))
        }, {
          default: function*(aoRecord, options = null) {
            var hash, serialized, vcRecord;
            vcRecord = aoRecord.constructor;
            serialized = (yield vcRecord.serialize(aoRecord, options));
            serialized.rev = void 0;
            hash = crypto.createHash('md5');
            hash.update(JSON.stringify(serialized));
            serialized._rev = hash.digest('hex');
            delete serialized.rev;
            return serialized;
          }
        }));

        _Class.initializeMixin();

        return _Class;

      }).call(this);
    }));
  };

}).call(this);
