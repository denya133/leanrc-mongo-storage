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

{ src, dest, task } = require 'gulp'
fs                  = require 'fs-extra'
coffee              = require 'gulp-coffee'

compileFiles = (originDir, destinationDir, mask = '**/*.coffee') ->
  new Promise (resolve, reject) ->
    opts = cwd: originDir
    fs.ensureDirSync destinationDir
    src mask, opts
      .pipe coffee()
      .pipe dest destinationDir
      .on 'error', reject
      .on 'end', resolve
    return

task 'compile_coffee', ->
  await compileFiles './lib', './dist'
