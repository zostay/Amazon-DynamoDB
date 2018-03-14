class Amazon::DynamoDB:ver<0.1>:auth<github:zostay>;
use v6;

use AWS::Session;
use AWS::Credentials;
use HTTP::UserAgent;

=begin pod

=head1 NAME

Amazon::DynamoDB - Low-level access to the DynamoDB API

=head1 SYNOPSIS

    use Amazon::DynamoDB;

    my $ddb = Amazon::DynamoDB.new

=head1 DESCRIPTION

This module provides the low-level API that interacts directly with DynamoDB.

The API is currently very primitive and will likely change to provide better
type-checking in the future.

=end pod

class GLOBAL::X::Amazon::DynamoDB::CommunicationError is Exception {
    has HTTP::Request $.request;
    has HTTP::Response $.response;

    method message() { "Communication Error" }
}

class GLOBAL::X::Amazon::DynamoDB::CRCError is Exception {
    has Int $.got-crc32;
    has Int $.expected-crc32;

    method message() { "Response failed CRC32 check, expected $!expected-crc32, but got $!got-crc32" }
}

has AWS::Session $.session is rw;
has AWS::Credentials $.credentials is rw;

has Str $.scheme = 'https';
has Str $.domain = 'amazonaws.com';

has HTTP::UserAgent $.ua .= new(:useragent("perl6-$?PACKAGE.^name()/$?PACKAGE.^ver()"));

method hostname() { "dynamodb.$.region.$!domain" }
method port() { "" }

method session() returns AWS::Session is rw {
    $!session //= AWS::Session.new;
    return-rw $!session;
}

method credentials() returns AWS::Credentials is rw {
    $!credentials //= load-credentials($.session);
    return-rw $!credentials;
}

method access-key() { $.credentials.access-key }
method secret-key() { $.credentials.secret-key }
method region()     { $.session.region }

method make-ddb-request($target, *%request) {
    use HTTP::Request::Common;
    use JSON::Tiny;
    use WebService::AWS::Auth::V4;

    my %crisp-request = %request.grep({ ?.value });

    my $body = to-json(%crisp-request);
    my $uri  = "$!scheme://$.hostname$.port/";

    my %headers =
        Host         => $.hostname,
        Content-Type => 'application/x-amz-json-1.0',
        X-Amz-Date   => amz-date-formatter(DateTime.now),
        X-Amz-Target => "DynamoDB_20120810.$target",
        ;

    my Str @headers = %headers.map({ "{.key}:{.value}" });

    my $v4 = WebService::AWS::Auth::V4.new(
        :method<POST>, :$body, :$uri, :@headers, :$.region, :service<dynamodb>,
        :access_key($.access-key), :secret($.secret-key)
    );

    my $authorization = $v4.signing-header.substr("Authorization: ".chars);
    %headers<Authorization> = $authorization;

    my $req = POST($uri, :content($body), |%headers);
    my $res = $!ua.request($req, :bin);

    if $res.is-success {
        use String::CRC32;

        my $request-id = $res.field('X-Amzn-RequestId').Str;
        my $crc32      = Int($res.field('X-Amz-Crc32').Str);

        my $got-crc32 = String::CRC32::crc32($res.content);

        if $crc32 != $got-crc32 {
            die X::Amazon::DynamoDB::CRCError.new(
                expected-crc32 => $crc32,
                got-crc32      => $got-crc32,
            );
        }

        my %response = from-json($res.decoded-content);
        %response<RequestId> = $request-id;

        return %response;
    }
    else {
        die X::Amazon::DynamoDB::CommunicationError.new(
            request  => $req,
            response => $res,
        );
    }
}

method BatchGetItem(
         :%RequestItems!,

    Str  :$ReturnConsumedCapacity,
) returns Hash {
    self.make-ddb-request('BatchGetItem',
        :%RequestItems,

        :$ReturnConsumedCapacity,
    );
}

method BatchWriteItem(
         :%RequestItems!,

    Str  :$ReturnConsumedCapacity,
    Str  :$ReturnItemCollectionMetrics,
) returns Hash {
    self.make-ddb-request('BatchWriteItems',
        :%RequestItems,

        :$ReturnConsumedCapacity,
        :$ReturnItemCollectionMetrics,
    );
}

method DeleteItem(
         :%Key!,
    Str  :$TableName!,

    Str  :$ConditionalOperator,
    Str  :$ConditionExpression,
    Str  :$Expected,
         :%ExpressionAttributeNames,
         :%ExpressionAttributeValues,
    Str  :$ReturnConsumedCapacity,
    Str  :$ReturnItemCollectionMetrics,
    Str  :$ReturnValues,
) returns Hash {
    self.make-ddb-request('DeleteItem',
        :%Key,
        :$TableName,

        :$ConditionalOperator,
        :$ConditionExpression,
        :$Expected,
        :%ExpressionAttributeNames,
        :%ExpressionAttributeValues,
        :$ReturnConsumedCapacity,
        :$ReturnItemCollectionMetrics,
        :$ReturnValues,
    );
}

method GetItem(
         :%Key!,
    Str  :$TableName!,

         :@AttributesToGet,
    Bool :$ConsistentRead,
         :%ExpressionAttributeNames,
    Str  :$ProjectionExpression,
    Str  :$ReturnConsumedCapacity,
) returns Hash {
    self.make-ddb-request('GetItem',
        :%Key,
        :$TableName,

        :@AttributesToGet,
        :$ConsistentRead,
        :%ExpressionAttributeNames,
        :$ProjectionExpression,
        :$ReturnConsumedCapacity,
    );
}

method PutItem(
         :%Item!,
    Str  :$TableName!,

    Str  :$ConditionalOperator,
    Str  :$ConditionExpression,
         :%Expected,
         :%ExressionAttributeNames,
         :%ExpressionAttributeValues,
    Str  :$ReturnConsumedCapacity,
    Str  :$ReturnItemCollectionMetrics,
    Str  :$ReturnValues,
) returns Hash {
    self.make-ddb-request('PutItem',
        :%Item,
        :$TableName,

        :$ConditionalOperator,
        :$ConditionExpression,
        :%Expected,
        :%ExressionAttributeNames,
        :%ExpressionAttributeValues,
        :$ReturnConsumedCapacity,
        :$ReturnItemCollectionMetrics,
        :$ReturnValues,
    );
}

method Query(
    Str  :$TableName!,

         :@AttributesToGet,
    Str  :$ConditionalOperator,
    Bool :$ConsistentRead,
         :%ExclusiveStartKey,
         :%ExpressionAttributeNames,
         :%ExpressionAttributeValues,
    Str  :$FilterExpression,
    Str  :$IndexName,
    Str  :$KeyConditionExpression,
         :%KeyConditions,
    Int  :$Limit,
    Str  :$ProjectionExpression,
         :%QueryFilter,
    Str  :$ReturnConsumedCapacity,
    Bool :$ScanIndexForward,
    Str  :$Select,
) returns Hash {
    self.make-ddb-request('Query',
        :$TableName,

        :@AttributesToGet,
        :$ConditionalOperator,
        :$ConsistentRead,
        :%ExclusiveStartKey,
        :%ExpressionAttributeNames,
        :%ExpressionAttributeValues,
        :$FilterExpression,
        :$IndexName,
        :$KeyConditionExpression,
        :%KeyConditions,
        :$Limit,
        :$ProjectionExpression,
        :%QueryFilter,
        :$ReturnConsumedCapacity,
        :$ScanIndexForward,
        :$Select,
    );
}

method Scan(
    Str  :$TableName!,

         :@AttributesToGet,
    Str  :$ConditionalOperator,
    Bool :$ConsistentRead,
         :%ExclusiveStartKey,
         :%ExpressionAttributeNames,
         :%ExpressionAttributeValues,
    Str  :$FilterExpression,
    Str  :$IndexName,
    Int  :$Limit,
    Str  :$ProjectionExpression,
         :%QueryFilter,
    Str  :$ReturnConsumedCapacity,
         :%ScanFilter,
    Int  :$Segment,
    Str  :$Select,
    Int  :$TotalSegments,
) returns Hash {
    self.make-ddb-request('Scan',
        :$TableName,

        :@AttributesToGet,
        :$ConditionalOperator,
        :$ConsistentRead,
        :%ExclusiveStartKey,
        :%ExpressionAttributeNames,
        :%ExpressionAttributeValues,
        :$FilterExpression,
        :$IndexName,
        :$Limit,
        :$ProjectionExpression,
        :%QueryFilter,
        :$ReturnConsumedCapacity,
        :%ScanFilter,
        :$Segment,
        :$Select,
        :$TotalSegments,
    );
}

method UpdateItem(
         :%Key!,
    Str  :$TableName!,

         :%AttributeUpdates,
    Str  :$ConditionalOperator,
    Str  :$ConditionExpression,
         :%Expected,
         :%ExpressionAttributeNames,
         :%ExpressionAttributeValues,
    Str  :$ReturnConsumedCapacity,
    Str  :$ReturnItemCollectionMetrics,
    Str  :$ReturnValues,
    Str  :$UpdateExpression,
) returns Hash {
    self.make-ddb-request('UpdateItem',
        :%Key,
        :$TableName,

        :%AttributeUpdates,
        :$ConditionalOperator,
        :$ConditionExpression,
        :%Expected,
        :%ExpressionAttributeNames,
        :%ExpressionAttributeValues,
        :$ReturnConsumedCapacity,
        :$ReturnItemCollectionMetrics,
        :$ReturnValues,
        :$UpdateExpression,
    );
}

method CreateTable(
         :@AttributeDefinitions!,
    Str  :$TableName!,
         :@KeySchema!,
         :%ProvisionedThroughput!,

         :@GlobalSecondaryIndexes,
         :@LocalSecondaryIndexes,
         :%SSESpecification,
         :%StreamSpecification,
) returns Hash {
    self.make-ddb-request('CreateTable',
        :@AttributeDefinitions,
        :$TableName,
        :@KeySchema,
        :%ProvisionedThroughput,

        :@GlobalSecondaryIndexes,
        :@LocalSecondaryIndexes,
        :%SSESpecification,
        :%StreamSpecification,
    );
}

method DeleteTable(
    Str :$TableName,
) returns Hash {
    self.make-ddb-request('DeleteTable', :$TableName);
}

method DescribeTable(
    Str  :$TableName!,
) returns Hash {
    self.make-ddb-request('DescribeTable', :$TableName);
}

method DescribeTimeToLive(
    Str  :$TableName!,
) returns Hash {
    self.make-ddb-request('DescribeTimeToLive', :$TableName);
}

method ListTables(
    Str  :$ExclusiveStartTableName,
    Int  :$Limit,
) returns Hash {
    self.make-ddb-request('ListTables',
        :$ExclusiveStartTableName,
        :$Limit,
    );
}

method UpdateTable(
    Str  :$TableName!,

         :@AttributeDefinitions,
         :@GlobalSecondaryIndexUpdates,
         :%ProvisionedThroughput,
         :%StreamSpecification,
) returns Hash {
    self.make-ddb-request('UpdateTable',
        :$TableName,

        :@AttributeDefinitions,
        :@GlobalSecondaryIndexUpdates,
        :%ProvisionedThroughput,
        :%StreamSpecification,
    );
}

method UpdateTimeToLive(
    Str  :$TableName!,
         :%TableToLiveSpecification!,
) returns Hash {
    self.make-ddb-request('UpdateTimeToLive',
        :$TableName,
        :%TableToLiveSpecification,
    );
}

method CreateGlobalTable(
    Str  :$GlobalTableName!,
         :@ReplicationGroup!,
) returns Hash {
    self.make-ddb-request('CreateGlobalTable',
        :$GlobalTableName,
        :@ReplicationGroup,
    );
}

method DescribeGlobalTable(
    Str  :$GlobalTableName!,
) returns Hash {
    self.make-ddb-request('DescribeGlobalTable',
        :$GlobalTableName,
    );
}

method ListGlobalTables(
    Str  :$ExclusiveStartGlobalTableName,
    Int  :$Limit,
    Str  :$RegionName,
) returns Hash {
    self.make-ddb-request('ListGlobalTables',
        :$ExclusiveStartGlobalTableName,
        :$Limit,
        :$RegionName,
    );
}

method UpdateGlobalTable(
    Str  :$GlobalTableName!,
         :@ReplicaUpdates!,
) returns Hash {
    self.make-ddb-request('UpdateGlobalTable',
        :$GlobalTableName,
        :@ReplicaUpdates,
    );
}

method ListTagsOfResource(
    Str  :$ResourceArn!,

    Str  :$NextToken,
) returns Hash {
    self.make-ddb-request('ListTagsOfResource',
        :$ResourceArn,

        :$NextToken,
    );
}

method TagResource(
    Str  :$ResourceArn!,
         :@Tags!,
) returns Hash {
    self.make-ddb-reqeust('TagResource',
        :$ResourceArn,
        :@Tags,
    );
}

method UntagResource(
    Str  :$ResourceArn!,
         :@TagKeys!,
) returns Hash {
    self.make-ddb-request('UntagResource',
        :$ResourceArn,
        :@TagKeys,
    );
}

method CreateBackup(
    Str  :$BackupName!,
    Str  :$TableName!,
) returns Hash {
    self.make-ddb-request('CreateBackup',
        :$BackupName,
        :$TableName,
    );
}

method DeleteBackup(
    Str  :$BackupArn!,
) returns Hash {
    self.make-ddb-request('DeleteBackup', :$BackupArn);
}

method DescribeBackup(
    Str  :$BackupArn!,
) returns Hash {
    self.make-ddb-request('DescribeBackup', :$BackupArn);
}

method DescribeContinuousBackups(
    Str  :$TableName!,
) returns Hash {
    self.make-ddb-request('DescribeContinuousBackups', :$TableName);
}

method ListBackups(
    Str  :$ExclusiveStartBackupArn,
    Int  :$Limit,
    Str  :$TableName,
    Int  :$TimeRangeLowerBound,
    Int  :$TimeRangeUpperBound,
) returns Hash {
    self.make-ddb-request('ListBackups',
        :$ExclusiveStartBackupArn,
        :$Limit,
        :$TableName,
        :$TimeRangeLowerBound,
        :$TimeRangeUpperBound,
    );
}

method RestoreTableFromBackup(
    Str  :$BackupArn!,
    Str  :$TargetTableName!,
) returns Hash {
    self.make-ddb-request('RestoreTableFromBackup',
        :$BackupArn,
        :$TargetTableName,
    );
}

method DescribeLimits() returns Hash {
    self.make-ddb-request('DescribeLimits');
}
