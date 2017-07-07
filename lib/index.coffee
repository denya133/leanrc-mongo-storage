# _         = require 'lodash'
# fs        = require 'fs'
LeanRC = require 'LeanRC'

###
Example of use

```coffee
LeanRC = require 'LeanRC'
MongoStorage = require 'leanrc-mongo-storage'

class TestApp extends LeanRC
  @inheritProtected()
  @include MongoStorage

  @const ANIMATE_ROBOT: Symbol 'animateRobot'
  @const ROBOT_SPEAKING: Symbol 'robotSpeaking'

  require('./controller/command/StartupCommand') TestApp
  require('./controller/command/PrepareControllerCommand') TestApp
  require('./controller/command/PrepareViewCommand') TestApp
  require('./controller/command/PrepareModelCommand') TestApp
  require('./controller/command/AnimateRobotCommand') TestApp

  require('./view/component/ConsoleComponent') TestApp
  require('./view/mediator/ConsoleComponentMediator') TestApp

  require('./model/proxy/RobotDataProxy') TestApp

  require('./AppFacade') TestApp


module.exports = TestApp.initialize().freeze()
```
###

Extension = (BaseClass) ->
  class MongoStorage extends BaseClass
    @inheritProtected()

    require('./iterator/MongoCursor') @Module

    require('./mixins/MongoCollectionMixin') @Module
    require('./mixins/MongoSerializerMixin') @Module
    require('./mixins/MongoMigrationMixin') @Module
  MongoStorage.initializeMixin()

sample = Extension LeanRC#::Module
Reflect.defineProperty Extension, 'reification',
  value: sample


module.exports = Extension
