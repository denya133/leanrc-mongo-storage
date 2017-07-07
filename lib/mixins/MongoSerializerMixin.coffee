crypto = require 'crypto'


module.exports = (Module)->
  Module.defineMixin Module::Serializer, (BaseClass) ->
    class MongoSerializerMixin extends BaseClass
      @inheritProtected()

      @public normalize: Function,
        default: (acRecord, ahPayload)->
          ahPayload.rev = ahPayload._rev
          ahPayload._rev = undefined
          delete ahPayload._rev
          acRecord.normalize ahPayload, @collection

      @public serialize: Function,
        default: (aoRecord, options = null)->
          vcRecord = aoRecord.constructor
          serialized = vcRecord.serialize aoRecord, options
          serialized.rev = undefined
          hash = crypto.createHash 'md5'
          hash.update JSON.stringify serialized
          serialized._rev = hash.digest 'hex'
          delete serialized.rev
          serialized


    MongoSerializerMixin.initializeMixin()
