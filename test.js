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

  t.test( "appending events", function( a ) {
    var eventStore = EventStore.create( DYNAMODB_TABLENAME
    , process.env.DYNAMODB_ENDPOINT )

    var aggregate = { id: "1" , version: 0 }

    var eventsToAppend = [
      { name: "Created", data: { name: "foo" } }
    , { name: "Deleted", data: {} }
    ]

    eventStore.append( aggregate, eventsToAppend, function() {
      eventStore.get( "1", 0, function( eventsAppended ) {
        a.equal( eventsAppended.length, 2, "should have appended 2 events" )
        var firstEvent = eventsAppended[ 0 ]
        a.equal( firstEvent.id, "1", "first event should have an aggregate id" )
        a.deepEqual( firstEvent.data, { name: "foo" }, "first event should have serialized data" )
        a.equal( firstEvent.version, 0, "first event should be version zero" )
        a.equal( firstEvent.name, "Created", "first event name should be Created" )
        a.ok( firstEvent.created < new Date().getTime(), "first event created value should be greater that current time" )
        a.end()
      } )
    } )
  } )

  t.end()
} )
