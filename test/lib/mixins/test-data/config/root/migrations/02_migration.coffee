LeanRC = require.main.require 'lib'

module.exports = (Module) ->
  class Migration2 extends Module
    @inheritProtected()
    @module Module
  Migration2.initialize()
