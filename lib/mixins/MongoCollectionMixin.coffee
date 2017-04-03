_             = require 'lodash'
RC            = require 'RC'

###
```coffee
# in application when its need

class MongoCollection extends LeanRC::Collection
  @include LeanRC::MongoCollectionMixin
```
###

# должен быть заиспользован модуль mongo-native-driver
# конекшены надо инкапсулировать скорее всего в атрибуте класса
# конфиги получать из @facade.retriveProxy 'Configuration' или из переменной инстанцирования коллекшена из @getData().configiration

# основную сложность при реализации данного модуля (Миксина) представляет адаптация дерективы $for в сочетании с $join - так как в монге нет понятия JOIN. в этом месте предположительно надо заиспользовать встроенный в монгу Aggregation Framework. Скорее всего через него же надо будет решить и вопрос с дерективами $collect, $aggregate, $avg и другие относящиеся к свертке массива.

# учесть также что в монге будет вестись работа с GridFS, а следовательно надо объявить некоторые методы в MongoCollectionMixin обворачивающих взаимодействие с GridFS.
module.exports = (MongoStorage)->
  class MongoStorage::MongoCollectionMixin extends RC::Mixin
    @inheritProtected()

    @Module: MongoStorage

    wrapReference = (value)->
      if _.isString(value) and /^[@]/.test value
        qb.ref value.replace '@', ''
      else
        qb value

    @public operatorsMap: Object,
      default:
        # Logical Query Operators
        $and: (args...)-> qb.and args...
        $or: (args...)-> qb.or args...
        $not: (args...)-> qb.not args...
        $nor: (args...)-> qb.not qb.or args... # not or # !(a||b) === !a && !b

        # Comparison Query Operators (aoSecond is NOT sub-query)
        $eq: (aoFirst, aoSecond)->
          qb.eq wrapReference(aoFirst), wrapReference(aoSecond) # ==
        $ne: (aoFirst, aoSecond)->
          qb.neq wrapReference(aoFirst), wrapReference(aoSecond) # !=
        $lt: (aoFirst, aoSecond)->
          qb.lt wrapReference(aoFirst), wrapReference(aoSecond) # <
        $lte: (aoFirst, aoSecond)->
          qb.lte wrapReference(aoFirst), wrapReference(aoSecond) # <=
        $gt: (aoFirst, aoSecond)->
          qb.gt wrapReference(aoFirst), wrapReference(aoSecond) # >
        $gte: (aoFirst, aoSecond)->
          qb.gte wrapReference(aoFirst), wrapReference(aoSecond) # >=
        $in: (aoFirst, alItems)-> # check value present in array
          qb.in wrapReference(aoFirst), qb alItems
        $nin: (aoFirst, alItems)-> # ... not present in array
          qb.notIn wrapReference(aoFirst), qb alItems

        # Array Query Operators
        $all: (aoFirst, alItems)-> # contains some values
          qb.and alItems.map (aoItem)->
            qb.in wrapReference(aoItem), wrapReference(aoFirst)
        $elemMatch: (aoFirst, aoSecond)-> # conditions for complex item
          wrappedReference = aoFirst.replace '@', ''
          voFilter = qb.and(aoSecond...).toAQL()
          voFirst = qb.expr "LENGTH(#{wrappedReference}[* FILTER #{voFilter}])"
          qb.gt voFirst, qb 0
        $size: (aoFirst, aoSecond)->
          voFirst = qb.expr "LENGTH(#{aoFirst.replace '@', ''})"
          qb.eq voFirst, wrapReference(aoSecond) # condition for array length

        # Element Query Operators
        $exists: (aoFirst, aoSecond)-> # condition for check present some value in field
          voFirst = qb.expr "HAS(#{aoFirst.replace '@', ''})"
          qb.eq voFirst, wrapReference(aoSecond)
        $type: (aoFirst, aoSecond)->
          voFirst = qb.expr "TYPENAME(#{aoFirst.replace '@', ''})"
          qb.eq voFirst, wrapReference(aoSecond) # check value type

        # Evaluation Query Operators
        $mod: (aoFirst, [divisor, remainder])->
          qb.eq qb.mod(wrapReference(aoFirst), qb divisor), qb remainder
        $regex: (aoFirst, aoSecond)-> # value must be string. ckeck it by RegExp.
          qb.expr "REGEX_TEST(#{aoFirst.replace '@', ''}, \"#{String aoSecond}\")"

        # Datetime Query Operators
        $td: (aoFirst, aoSecond)-> # this day (today)
          todayStart = moment().startOf 'day'
          todayEnd = moment().endOf 'day'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), todayStart.toISOString()
              qb.lt wrapReference(aoFirst), todayEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), todayStart.toISOString()
              qb.lt wrapReference(aoFirst), todayEnd.toISOString()
            ]...
        $ld: (aoFirst, aoSecond)-> # last day (yesterday)
          yesterdayStart = moment().subtract(1, 'days').startOf 'day'
          yesterdayEnd = moment().subtract(1, 'days').endOf 'day'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), yesterdayStart.toISOString()
              qb.lt wrapReference(aoFirst), yesterdayEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), yesterdayStart.toISOString()
              qb.lt wrapReference(aoFirst), yesterdayEnd.toISOString()
            ]...
        $tw: (aoFirst, aoSecond)-> # this week
          weekStart = moment().startOf 'week'
          weekEnd = moment().endOf 'week'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), weekStart.toISOString()
              qb.lt wrapReference(aoFirst), weekEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), weekStart.toISOString()
              qb.lt wrapReference(aoFirst), weekEnd.toISOString()
            ]...
        $lw: (aoFirst, aoSecond)-> # last week
          weekStart = moment().subtract(1, 'weeks').startOf 'week'
          weekEnd = weekStart.clone().endOf 'week'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), weekStart.toISOString()
              qb.lt wrapReference(aoFirst), weekEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), weekStart.toISOString()
              qb.lt wrapReference(aoFirst), weekEnd.toISOString()
            ]...
        $tm: (aoFirst, aoSecond)-> # this month
          firstDayStart = moment().startOf 'month'
          lastDayEnd = moment().endOf 'month'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
        $lm: (aoFirst, aoSecond)-> # last month
          firstDayStart = moment().subtract(1, 'months').startOf 'month'
          lastDayEnd = firstDayStart.clone().endOf 'month'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
        $ty: (aoFirst, aoSecond)-> # this year
          firstDayStart = moment().startOf 'year'
          lastDayEnd = firstDayStart.clone().endOf 'year'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
        $ly: (aoFirst, aoSecond)-> # last year
          firstDayStart = moment().subtract(1, 'years').startOf 'year'
          lastDayEnd = firstDayStart.clone().endOf 'year'
          if aoSecond
            qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...
          else
            qb.not qb.and [
              qb.gte wrapReference(aoFirst), firstDayStart.toISOString()
              qb.lt wrapReference(aoFirst), lastDayEnd.toISOString()
            ]...

    @public parseFilter: Function,
      args: [Object]
      return: RC::Constants.ANY
      default: ({field, parts, operator, operand, implicitField})->
        if field? and operator isnt '$elemMatch' and parts.length is 0
          @operatorsMap[operator] field, operand
        else if field? and operator is '$elemMatch'
          if implicitField is yes
            @operatorsMap[operator] field, parts.map (part)=>
              part.field = "@CURRENT"
              @parseFilter part
          else
            @operatorsMap[operator] field, parts.map (part)=>
              part.field = "@CURRENT.#{part.field}"
              @parseFilter part
        else
          @operatorsMap[operator ? '$and'] parts.map @parseFilter.bind @

    @public parseQuery: Function,
      default: (aoQuery)->
        voQuery = null
        aggUsed = aggPartial = intoUsed = intoPartial = finAggUsed = finAggPartial = null
        if aoQuery.$remove?
          do =>
            if aoQuery.$forIn?
              for own asItemRef, asCollectionFullName of aoQuery.$forIn
                voQuery = (voQuery ? qb).for qb.ref asItemRef.replace '@', ''
                  .in asCollectionFullName
              if (voJoin = aoQuery.$join)?
                vlJoinFilters = voJoin.$and.map (asItemRef, {$eq:asRelValue})->
                  voItemRef = qb.ref asItemRef.replace '@', ''
                  voRelValue = qb.ref asRelValue.replace '@', ''
                  qb.eq voItemRef, voRelValue
                voQuery = voQuery.filter qb.and vlJoinFilters...
              if (voFilter = aoQuery.$filter)?
                voQuery = voQuery.filter @parseFilter Parser.parse voFilter
              if (voLet = aoQuery.$let)?
                for own asRef, aoValue of voLet
                  voQuery = (voQuery ? qb).let qb.ref(asRef.replace '@', ''), qb.expr @parseQuery LeanRC::Query.new aoValue
              voQuery = (voQuery ? qb).remove aoQuery.$remove
              if aoQuery.$into?
                voQuery = voQuery.into aoQuery.$into
        else if (voRecord = aoQuery.$insert)?
          do =>
            if aoQuery.$into?
              if aoQuery.$forIn?
                for own asItemRef, asCollectionFullName of aoQuery.$forIn
                  voQuery = (voQuery ? qb).for qb.ref asItemRef.replace '@', ''
                    .in asCollectionFullName
                if (voJoin = aoQuery.$join?.$and)?
                  vlJoinFilters = voJoin.map (asItemRef, {$eq:asRelValue})->
                    voItemRef = qb.ref asItemRef.replace '@', ''
                    voRelValue = qb.ref asRelValue.replace '@', ''
                    qb.eq voItemRef, voRelValue
                  voQuery = voQuery.filter qb.and vlJoinFilters...
                if (voFilter = aoQuery.$filter)?
                  voQuery = voQuery.filter @parseFilter Parser.parse voFilter
                if (voLet = aoQuery.$let)?
                  for own asRef, aoValue of voLet
                    voQuery = (voQuery ? qb).let qb.ref(asRef.replace '@', ''), qb.expr @parseQuery LeanRC::Query.new aoValue
              vhObjectForInsert = @serializer.serialize voRecord
              voQuery = (voQuery ? qb).insert vhObjectForInsert
                .into aoQuery.$into
        else if (voRecord = aoQuery.$update)?
          do =>
            if aoQuery.$into?
              if aoQuery.$forIn?
                for own asItemRef, asCollectionFullName of aoQuery.$forIn
                  voQuery = (voQuery ? qb).for qb.ref asItemRef.replace '@', ''
                    .in asCollectionFullName
                if (voJoin = aoQuery.$join?.$and)?
                  vlJoinFilters = voJoin.map (asItemRef, {$eq:asRelValue})->
                    voItemRef = qb.ref asItemRef.replace '@', ''
                    voRelValue = qb.ref asRelValue.replace '@', ''
                    qb.eq voItemRef, voRelValue
                  voQuery = voQuery.filter qb.and vlJoinFilters...
                if (voFilter = aoQuery.$filter)?
                  voQuery = voQuery.filter @parseFilter Parser.parse voFilter
                if (voLet = aoQuery.$let)?
                  for own asRef, aoValue of voLet
                    voQuery = (voQuery ? qb).let qb.ref(asRef.replace '@', ''), qb.expr @parseQuery LeanRC::Query.new aoValue
              vhObjectForUpdate = @serializer.serialize voRecord
              voQuery = (voQuery ? qb).update qb.ref 'doc'
                .with vhObjectForUpdate
                .into aoQuery.$into
        else if (voRecord = aoQuery.$replace)?
          do =>
            if aoQuery.$into?
              if aoQuery.$forIn?
                for own asItemRef, asCollectionFullName of aoQuery.$forIn
                  voQuery = (voQuery ? qb).for qb.ref asItemRef.replace '@', ''
                    .in asCollectionFullName
                if (voJoin = aoQuery.$join?.$and)?
                  vlJoinFilters = voJoin.map (asItemRef, {$eq:asRelValue})->
                    voItemRef = qb.ref asItemRef.replace '@', ''
                    voRelValue = qb.ref asRelValue.replace '@', ''
                    qb.eq voItemRef, voRelValue
                  voQuery = voQuery.filter qb.and vlJoinFilters...
                if (voFilter = aoQuery.$filter)?
                  voQuery = voQuery.filter @parseFilter Parser.parse voFilter
                if (voLet = aoQuery.$let)?
                  for own asRef, aoValue of voLet
                    voQuery = (voQuery ? qb).let qb.ref(asRef.replace '@', ''), qb.expr @parseQuery LeanRC::Query.new aoValue
              vhObjectForReplace = @serializer.serialize voRecord
              voQuery = (voQuery ? qb).replace qb.ref 'doc'
                .with vhObjectForReplace
                .into aoQuery.$into
        else if aoQuery.$forIn?
          do =>
            for own asItemRef, asCollectionFullName of aoQuery.$forIn
              voQuery = (voQuery ? qb).for qb.ref asItemRef.replace '@', ''
                .in asCollectionFullName
            if (voJoin = aoQuery.$join)?
              vlJoinFilters = voJoin.$and.map (asItemRef, {$eq:asRelValue})->
                voItemRef = qb.ref asItemRef.replace '@', ''
                voRelValue = qb.ref asRelValue.replace '@', ''
                qb.eq voItemRef, voRelValue
              voQuery = voQuery.filter qb.and vlJoinFilters...
            if (voFilter = aoQuery.$filter)?
              voQuery = voQuery.filter @parseFilter Parser.parse voFilter
            if (voLet = aoQuery.$let)?
              for own asRef, aoValue of voLet
                voQuery = (voQuery ? qb).let qb.ref(asRef.replace '@', ''), qb.expr @parseQuery LeanRC::Query.new aoValue
            if (voCollect = aoQuery.$collect)?
              for own asRef, aoValue of voCollect
                voQuery = voQuery.collect qb.ref(asRef.replace '@', ''), qb.expr @parseQuery LeanRC::Query.new aoValue
            if (voAggregate = aoQuery.$aggregate)?
              vsAggregate = (for own asRef, aoValue of voAggregate
                do (asRef, aoValue)->
                  "#{asRef.replace '@', ''} = #{aoValue}"
              ).join ', '
              aggUsed = _.escapeRegExp "FILTER {{AGGREGATE #{vsAggregate}}}"
              if aoQuery.$collect?
                aggPartial = "AGGREGATE #{vsAggregate}"
              else
                aggPartial = "COLLECT AGGREGATE #{vsAggregate}"
              voQuery = voQuery.filter qb.expr "{{AGGREGATE #{vsAggregate}}}"
            if (vsInto = aoQuery.$into)?
              intoUsed = _.escapeRegExp "FILTER {{INTO #{vsInto}}}"
              intoPartial = "INTO #{vsInto}"
              query = query.filter qb.expr "{{INTO #{vsInto}}}"
            if (voHaving = aoQuery.$having)?
              voQuery = voQuery.filter @parseFilter Parser.parse voHaving
            if (voSort = aoQuery.$sort)?
              for own asRef, asSortDirect of aoQuery.$sort
                voQuery = voQuery.sort qb.ref(asRef.replace '@', ''), asSortDirect

            if (vnLimit = aoQuery.$limit)?
              if (vnOffset = aoQuery.$offset)?
                voQuery = voQuery.limit vnOffset, vnLimit
              else
                voQuery = voQuery.limit vnLimit

            if (aoQuery.$count)?
              voQuery = voQuery.collectWithCountInto 'counter'
                .return qb.ref('counter').then('counter').else('0')
            else if (vsSum = aoQuery.$sum)?
              finAggUsed = "RETURN {{COLLECT AGGREGATE result = SUM\\(TO_NUMBER\\(#{vsSum.replace '@', ''}\\)\\) RETURN result}}"
              finAggPartial = "COLLECT AGGREGATE result = SUM(TO_NUMBER(#{vsSum.replace '@', ''})) RETURN result"
              voQuery = voQuery.return qb.expr "{{#{finAggPartial}}}"
            else if (vsMin = aoQuery.$min)?
              voQuery = voQuery.sort qb.ref(vsMin.replace '@', '')
                .limit 1
                .return qb.ref(vsMin.replace '@', '')
            else if (vsMax = aoQuery.$max)?
              voQuery = voQuery.sort qb.ref(vsMax.replace '@', ''), 'DESC'
                .limit 1
                .return qb.ref(vsMax.replace '@', '')
            else if (vsAvg = aoQuery.$avg)?
              finAggUsed = "RETURN {{COLLECT AGGREGATE result = AVG\\(TO_NUMBER\\(#{vsSum.replace '@', ''}\\)\\) RETURN result}}"
              finAggPartial = "COLLECT AGGREGATE result = AVG(TO_NUMBER(#{vsSum.replace '@', ''})) RETURN result"
              voQuery = voQuery.return qb.expr "{{#{finAggPartial}}}"
            else
              if aoQuery.$return?
                voReturn = if _.isString aoQuery.$return
                  qb.ref aoQuery.$return.replace '@', ''
                else if _.isObject aoQuery.$return
                  vhObj = {}
                  for own key, value of aoQuery.$return
                    do (key, value)->
                      vhObj[key] = qb.ref value.replace '@', ''
                  vhObj
                if aoQuery.$distinct
                  voQuery = voQuery.returnDistinct voReturn
                else
                  voQuery = voQuery.return voReturn
        vsQuery = voQuery.toAQL()

        if aggUsed and new RegExp(aggUsed).test vsQuery
          vsQuery = vsQuery.replace new RegExp(aggUsed), aggPartial
        if intoUsed and new RegExp(intoUsed).test vsQuery
          vsQuery = vsQuery.replace new RegExp(intoUsed), intoPartial
        if finAggUsed and new RegExp(finAggUsed).test vsQuery
          vsQuery = vsQuery.replace new RegExp(finAggUsed), finAggPartial

        return vsQuery

    @public executeQuery: Function,
      default: (asQuery, options)->
        voNativeCursor = db._query asQuery
        voCursor = LeanRC::Cursor.new @delegate, voNativeCursor
        return voCursor


  return MongoStorage::MongoCollectionMixin.initialize()
