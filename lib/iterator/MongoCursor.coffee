_  = require 'lodash'


module.exports = (Module)->
  {
    ANY

    Collection
    CoreObject
    CursorInterface
  } = Module::

  class MongoCursor extends CoreObject
    @inheritProtected()

    @implements CursorInterface

    @module Module

    ipoCursor = @private cursor: ANY
    ipoCollection = @private collection: Collection

    @public setCursor: Function,
      args: [ANY]
      return: CursorInterface
      default: (aoCursor)->
        @[ipoCursor] = aoCursor
        return @

    @public setCollection: Function,
      default: (aoCollection)->
        @[ipoCollection] = aoCollection
        return @

    @public @async toArray: Function,
      default: (acRecord = null)->
        while yield @hasNext()
          yield @next acRecord

    @public @async next: Function,
      default: (acRecord = null)->
        acRecord ?= @[ipoCollection]?.delegate
        data = yield @[ipoCursor].next()
        if acRecord?
          if data?
            acRecord.new data
          else
            data
        else
          data

    @public @async hasNext: Function,
      default: -> yield @[ipoCursor].hasNext()

    @public @async close: Function,
      default: -> yield @[ipoCursor].close()

    @public @async count: Function,
      default: -> yield @[ipoCursor].count()

    @public @async forEach: Function,
      default: (lambda, acRecord = null)->
        index = 0
        try
          while yield @hasNext()
            yield lambda (yield @next acRecord), index++
          return
        catch err
          yield @close()
          throw err

    @public @async map: Function,
      default: (lambda, acRecord = null)->
        index = 0
        try
          while yield @hasNext()
            yield lambda (yield @next acRecord), index++
        catch err
          yield @close()
          throw err

    @public @async filter: Function,
      default: (lambda, acRecord = null)->
        index = 0
        records = []
        try
          while yield @hasNext()
            record = yield @next acRecord
            if yield lambda record, index++
              records.push record
          records
        catch err
          yield @close()
          throw err

    @public @async find: Function,
      default: (lambda, acRecord = null)->
        index = 0
        _record = null
        try
          while yield @hasNext()
            record = yield @next acRecord
            if yield lambda record, index++
              _record = record
              break
          _record
        catch err
          yield @close()
          throw err

    @public @async compact: Function,
      default: (acRecord = null)->
        acRecord ?= @[ipoCollection]?.delegate
        index = 0
        records = []
        try
          while yield @hasNext()
            rawRecord = yield @[ipoCursor].next()
            unless _.isEmpty rawRecord
              record = acRecord.new rawRecord
              records.push record
          records
        catch err
          yield @close()
          throw err

    @public @async reduce: Function,
      default: (lambda, initialValue, acRecord = null)->
        try
          index = 0
          _initialValue = initialValue
          while yield @hasNext()
            _initialValue = yield lambda _initialValue, (yield @next acRecord), index++
          _initialValue
        catch err
          yield @close()
          throw err

    @public @async first: Function,
      default: (acRecord = null)->
        try
          if yield @hasNext()
            yield @next acRecord
          else
            null
        catch err
          yield @close()
          throw err

    @public init: Function,
      default: (aoCollection, aoCursor = null)->
        @super arguments...
        @[ipoCollection] = aoCollection
        @[ipoCursor] = aoCursor


  MongoCursor.initialize()
