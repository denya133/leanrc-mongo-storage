# _         = require 'lodash'
# fs        = require 'fs'
RC = require 'RC'


class MongoStorage extends RC::Module
  @inheritProtected()
  # Utils: {}
  # Scripts: {}
  require('./Constants') MongoStorage

  require('./mixins/MongoCollectionMixin') MongoStorage


module.exports = MongoStorage.initialize()
