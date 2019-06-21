#!/usr/bin/env node

'use strict'

const _$filter = require('lodash/filter')
const _$find = require('lodash/find')
const _$isEmpty = require('lodash/isEmpty')
const _$isUndefined = require('lodash/isUndefined')
const _$map = require('lodash/map')
const astTypes = require('ast-types')
const fs = require('fs')
const path = require('path')
const recast = require('recast')
const thriftParser = require('@creditkarma/thrift-parser')
const yargs = require('yargs')

const _thriftReturnTypeHasThriftMap = (typeAst) => {
  if (typeAst.type === 'MapType') { return true }
  if (! _$isUndefined(typeAst.valueType)) {
    return _thriftReturnTypeHasThriftMap(typeAst.valueType)
  }
  if (! _$isUndefined(typeAst.keyType)) {
    return _thriftReturnTypeHasThriftMap(typeAst.keyType)
  }
  return false
}

const processFile = (thriftFilePath, javascriptFilePath, thriftSource, javascriptSource) => {
  const thriftSourceAst = thriftParser.parse(thriftSource)
  const thriftConcourseServiceDefinitionAst = _$find(thriftSourceAst.body, {
    name: { value: 'ConcourseService' },
    type: 'ServiceDefinition'
  })
  const { functions: thriftConcourseServiceMethodsAst } = thriftConcourseServiceDefinitionAst
  let methodsWithMapsInReturnValueAst = _$filter(thriftConcourseServiceMethodsAst, (methodAst) => {//
    return _thriftReturnTypeHasThriftMap(methodAst.returnType)
  })
  const methodsWithMapsInReturnValue = _$map(methodsWithMapsInReturnValueAst, (methodAst) => methodAst.name.value)
  let methodResultClassesToCheck = _$map(methodsWithMapsInReturnValue, (methodName) => `ConcourseService_${methodName}_result`)
  methodResultClassesToCheck = new Set(methodResultClassesToCheck)
  const javascriptSourceAst = recast.parse(javascriptSource)
  const javascriptSourceAstNodePath = new astTypes.NodePath(javascriptSourceAst.program)
  javascriptSourceAstNodePath
    .get('body')
    .each((path) => {
      if (astTypes.namedTypes.VariableDeclaration.check(path.node)) {
        path
          .get('declarations')
          .each((path) => {
            if (! astTypes.namedTypes.VariableDeclarator.check(path.node)) {
              return
            }
            const pathIdPath = path.get('id')
            if (! astTypes.namedTypes.Identifier.check(pathIdPath.node)) {
              return
            }
            if (methodResultClassesToCheck.has(pathIdPath.node.name)) {
              console.log(`In method result class "${pathIdPath.node.name}"`)
              path
                .get('init', 'body', 'body')
                .each((path) => {
                  if (! astTypes.namedTypes.MethodDefinition.check(path.node)) {
                    return
                  }
                  const pathKeyPath = path.get('key')
                  if (! astTypes.namedTypes.Identifier.check(pathKeyPath.node)) {
                    return
                  }
                  if (pathKeyPath.node.name === 'read') {
                    console.log(`    > In method "read" of method result class "${pathIdPath.node.name}"`)
                    const emptyObjectAssignmentsFound = {}
                    const mapVariableNames = new Set()
                    const readMethodBlockPath = path.get('value', 'body', 'body')
                    astTypes.visit(readMethodBlockPath.node, {
                      visitAssignmentExpression(path) {
                        try {
                          if (path.node.operator !== '=') {
                            this.abort()
                          }
                          const pathRightPath = path.get('right')
                          if ((! astTypes.namedTypes.ObjectExpression.check(pathRightPath.node)) ||
                              (! _$isEmpty(pathRightPath.node.properties))) {
                            this.abort()
                          }
                          emptyObjectAssignmentsFound[path.node.loc.start.line] = path
                        } catch (error) {
                          if (error instanceof this.AbortRequest) {
                            return false
                          } else {
                            throw error
                          }
                        }
                        this.traverse(path)
                      }
                    })
                    astTypes.visit(readMethodBlockPath.node, {
                      visitCallExpression(path) {
                        try {
                          const pathCalleePath = path.get('callee')
                          if (! astTypes.namedTypes.MemberExpression.check(pathCalleePath.node)) {
                            this.abort()
                          }
                          const { code: parsedPathCallee } = recast.print(pathCalleePath.node)
                          if (parsedPathCallee !== 'input.readMapBegin') {
                            this.abort()
                          }
                          const emptyObjectAssignmentPath = emptyObjectAssignmentsFound[pathCalleePath.node.loc.start.line - 1]
                          if (_$isUndefined(emptyObjectAssignmentPath)) {
                            this.abort()
                          }
                          const emptyObjectAssignmentPathLeftPath = emptyObjectAssignmentPath.get('left')
                          const { code: emptyObjectAssignmentVariableName } = recast.print(emptyObjectAssignmentPathLeftPath.node)
                          mapVariableNames.add(emptyObjectAssignmentVariableName)
                          const emptyObjectAssignmentPathRightPath = emptyObjectAssignmentPath.get('right')
                          emptyObjectAssignmentPathRightPath.replace(
                            astTypes.builders.newExpression(
                              astTypes.builders.identifier('Map'),
                              []))
                        } catch (error) {
                          if (error instanceof this.AbortRequest) {
                            return false
                          } else {
                            throw error
                          }
                        }
                        this.traverse(path)
                      }
                    })
                    astTypes.visit(readMethodBlockPath.node, {
                      visitMemberExpression(path) {
                        try {
                          debugger
                          // const { code: memberExpressionValue } = recast.print(path.node)
                          const pathObjectPath = path.get('object')
                          const { code: memberExpressionValue } = recast.print(pathObjectPath.node)
                          // if () {
                          //   this.abort()
                          // }
                          console.log(`memberExpressionValue === ${memberExpressionValue}`)
                        } catch (error) {
                          if (error instanceof this.AbortRequest) {
                            return false
                          } else {
                            throw error
                          }
                        }
                        this.traverse(path)
                      }
                    })
                    //
                  }
                })
              console.log('\n')
            }
          })
      }
    })
  const updatedJavascriptSource = recast
    .print(javascriptSourceAst)
    .code
  fs.writeFileSync(javascriptFilePath, updatedJavascriptSource, 'utf8')
}

const main = (argv) => {
  yargs
    .command({
      builder: (yargs) => {
        yargs
          .positional('javascript-file', {
            coerce: (filePath) => {
              return path.resolve(filePath)
            }
          })
          .positional('thrift-file', {
            coerce: (filePath) => {
              return path.resolve(filePath)
            }
          })
          .version(false)
      },
      command: '$0 <thrift-file> <javascript-file>',
      desc: 'Parses JavaScript code generated by Thrift and patches all types that were Thrift maps to actual JavaScript maps',
      handler: (argv) => {
        const {
          javascriptFile: javascriptFilePath,
          thriftFile: thriftFilePath
        } = argv
        const javascriptSource = fs.readFileSync(javascriptFilePath, 'utf8')
        const thriftSource = fs.readFileSync(thriftFilePath, 'utf8')
        processFile(thriftFilePath, javascriptFilePath, thriftSource, javascriptSource)
      }
    })
    .parse(argv)
}

const argv = process.argv.slice(2)
main(argv)
