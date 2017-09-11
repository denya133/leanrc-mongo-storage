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

    @public isClosed: Boolean,
      default: false
      get: ()->
        @[ipoCursor].isClosed()

    @public setIterable: Function,
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
      default: ->
        while yield @hasNext()
          yield @next()

    @public @async next: Function,
      default: ->
        data = yield @[ipoCursor].next()
        switch
          when not data?
            yield return data
          when @[ipoCollection]?
            yield return @[ipoCollection]?.normalize data
          else
            yield return data

    @public @async hasNext: Function,
      default: -> yield not @isClosed and (yield @[ipoCursor].hasNext())

    @public @async close: Function,
      default: -> yield @[ipoCursor].close()

    @public @async count: Function,
      default: -> yield @[ipoCursor].count yes

    @public @async forEach: Function,
      default: (lambda)->
        index = 0
        try
          while yield @hasNext()
            yield lambda (yield @next()), index++
          return
        catch err
          yield @close()
          throw err

    @public @async map: Function,
      default: (lambda)->
        index = 0
        try
          while yield @hasNext()
            yield lambda (yield @next()), index++
        catch err
          yield @close()
          throw err

    @public @async filter: Function,
      default: (lambda)->
        index = 0
        records = []
        try
          while yield @hasNext()
            record = yield @next()
            if yield lambda record, index++
              records.push record
          records
        catch err
          yield @close()
          throw err

    @public @async find: Function,
      default: (lambda)->
        index = 0
        _record = null
        try
          while yield @hasNext()
            record = yield @next()
            if yield lambda record, index++
              _record = record
              break
          _record
        catch err
          yield @close()
          throw err

    @public @async compact: Function,
      default: ()->
        records = []
        try
          while yield @hasNext()
            rawRecord = yield @[ipoCursor].next()
            unless _.isEmpty rawRecord
              record = if @[ipoCollection]?
                @[ipoCollection].normalize rawResult
              else
                rawResult
              records.push record
          records
        catch err
          yield @close()
          throw err

    @public @async reduce: Function,
      default: (lambda, initialValue)->
        try
          index = 0
          _initialValue = initialValue
          while yield @hasNext()
            _initialValue = yield lambda _initialValue, (yield @next()), index++
          _initialValue
        catch err
          yield @close()
          throw err

    @public @async first: Function,
      default: ()->
        try
          result = if yield @hasNext()
            yield @next()
          else
            null
          yield @close()
          yield return result
        catch err
          yield @close()
          throw err

    @public @static @async restoreObject: Function,
      default: ->
        throw new Error "restoreObject method not supported for #{@name}"
        yield return

    @public @static @async replicateObject: Function,
      default: ->
        throw new Error "replicateObject method not supported for #{@name}"
        yield return

    @public init: Function,
      default: (aoCollection = null, aoCursor = null)->
        @super arguments...
        @[ipoCollection] = aoCollection
        @[ipoCursor] = aoCursor

  MongoCursor.initialize()
