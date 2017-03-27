var AWS = require( "aws-sdk" )
var Tap = require( "tap" )

var EventStore = require( "./index.js" )

var DYNAMODB_TABLENAME = "events"

Tap.test( "dynamodb event store", function( t ) {

  var db = new AWS.DynamoDB( {
    endpoint: process.env.DYNAMODB_ENDPOINT
  } )

  t.beforeEach( function( callback ) {
    var tableParams = {
      TableName: DYNAMODB_TABLENAME
    , AttributeDefinitions: [
        { AttributeName: "id" , AttributeType: "S" }
      , { AttributeName: "version" , AttributeType: "N" }
      ]
    , KeySchema: [
        { AttributeName: "id", KeyType: "HASH" }
      , { AttributeName: "version", KeyType: "RANGE" }
      ]
    , ProvisionedThroughput: { ReadCapacityUnits: 1 , WriteCapacityUnits: 1 }
    }

    console.log( "creating test table" )
    db.createTable( tableParams, callback )
  } )

  t.afterEach( function( callback ) {
    console.log( "deleting test table" )
    db.deleteTable( { TableName: DYNAMODB_TABLENAME }, callback )
  } )

  t.test( "appending an event", function( a ) {
    var es = EventStore.create(
      DYNAMODB_TABLENAME
    , process.env.DYNAMODB_ENDPOINT )

    var event = { version: 1, name: "Created", id: "1", data: { name: "foo" } }

    es.append( function( err ) {
      a.notOk( err )
      es.get( function( events ) {
        a.equal( events[0].id, "1", "first event should have an aggregate id" )
        a.deepEqual( events[0].data, { name: "foo" }, "first event should have serialized data" )
        a.equal( events[0].version, 1, "first event should be version zero" )
        a.equal( events[0].name, "Created", "first event name should be Created" )
        a.end()
      }, "1", 0 )
    }, event )
  } )

  t.test( "getting all events", function( a ) {
    var es = EventStore.create(
      DYNAMODB_TABLENAME
    , process.env.DYNAMODB_ENDPOINT )

    var event1 = { version: 1, name: "Created", id: "1", data: { name: "foo" } }
    var event2 = { version: 1, name: "Created", id: "2", data: { name: "bar" } }

    es.append( function( err ) {
      a.notOk( err )
      es.append( function( err ) {
        a.notOk( err )
        es.getAll( function( events ) {
          a.equal( events.length, 2 )
          a.end()
        } )
      }, event2 )
    }, event1 )
  } )
  t.end()
} )
