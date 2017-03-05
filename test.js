var AWS = require( "aws-sdk" )
var Tap = require( "tap" )

var EventStore = require( "./index.js" )

var DYNAMODB_REGION = "local"
var DYNAMODB_ENDPOINT = "http://database:8000"
var DYNAMODB_TABLENAME = "events"

Tap.test( "dynamodb event store", function( t ) {

  var db = new AWS.DynamoDB( {
    region: DYNAMODB_REGION
  , endpoint: DYNAMODB_ENDPOINT
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
    , ProvisionedThroughput: {
        ReadCapacityUnits: 5
      , WriteCapacityUnits: 5
      }
    }

    db.createTable( tableParams, function( err, data ) {
      if ( err )
        console.log( err.message )

      callback()
    } )
  } )

  t.afterEach( function( callback ) {
    db.deleteTable( { TableName: DYNAMODB_TABLENAME }, function( err, data ) {
      if ( err )
        console.log( err.message )

      callback()
    } )
  } )

  t.test( "appending events", function( a ) {
    var eventStore = EventStore.create( {
        region: DYNAMODB_REGION
      , endpoint: DYNAMODB_ENDPOINT
      , tableName: DYNAMODB_TABLENAME
    } )

    var aggregate = {
      id: "1"
    , version: 0
    }

    var eventsToAppend = [
      { name: "Created", data: { name: "foo" } }
    , { name: "Deleted", data: {} }
    ]

    eventStore.append( aggregate, eventsToAppend, function() {
      eventStore.get( "1", 0, function( eventsAppended ) {
        a.equal( eventsAppended.length, 2 )
        var firstEvent = eventsAppended[ 0 ]
        a.equal( firstEvent.id, "1" )
        a.deepEqual( firstEvent.data, { name: "foo" } )
        a.equal( firstEvent.version, 0 )
        a.equal( firstEvent.name, "Created" )
        a.ok( firstEvent.created < new Date().getTime() )
        a.end()
      } )
    } )
  } )

  t.end()
} )
