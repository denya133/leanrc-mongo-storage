crypto = require 'crypto'


module.exports = (Module)->
  {
    AnyT
    FuncG, SubsetG
    RecordInterface
    Mixin
    Serializer
  } = Module::

  Module.defineMixin Mixin 'MongoSerializerMixin', (BaseClass = Serializer) ->
    class extends BaseClass
      @inheritProtected()

      @public @async normalize: FuncG([SubsetG(RecordInterface), AnyT], RecordInterface),
        default: (acRecord, ahPayload)->
          ahPayload.rev = ahPayload._rev
          ahPayload._rev = undefined
          delete ahPayload._rev
          return yield acRecord.normalize ahPayload, @collection

      @public @async serialize: FuncG([RecordInterface, Object], AnyT),
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
