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

crypto = require 'crypto'

module.exports = (Module)->
  {
    AnyT
    FuncG, SubsetG, MaybeG
    RecordInterface
    Mixin
    Serializer
  } = Module::

  Module.defineMixin Mixin 'MongoSerializerMixin', (BaseClass = Serializer) ->
    class extends BaseClass
      @inheritProtected()

      @public @async normalize: FuncG([SubsetG(RecordInterface), MaybeG AnyT], RecordInterface),
        default: (acRecord, ahPayload)->
          ahPayload.rev = ahPayload._rev
          ahPayload._rev = undefined
          delete ahPayload._rev
          return yield acRecord.normalize ahPayload, @collection

      @public @async serialize: FuncG([MaybeG(RecordInterface), MaybeG Object], MaybeG AnyT),
        default: (aoRecord, options = null)->
          vcRecord = aoRecord.constructor
          serialized = yield vcRecord.serialize aoRecord, options
          serialized.rev = undefined
          hash = crypto.createHash 'md5'
          hash.update JSON.stringify serialized
          serialized._rev = hash.digest 'hex'
          delete serialized.rev
          yield return serialized


      @initializeMixin()
