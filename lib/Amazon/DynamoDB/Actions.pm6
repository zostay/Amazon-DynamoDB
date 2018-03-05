unit class Amazon::DynamoDB::Actions;
use v6;

use HTTP::UserAgent;

=begin pod

=head1 NAME

Amazon::DynamoDB::Actions - Internal API helper

=head1 DESCRIPTION

This module provides the low-level API that interacts directly with DynamoDB. This API is expected to change rapidly, so no documentation is provided.

Use at your own risk.

=end pod

class GLOBAL::X::Amazon::DynamoDB::Actions::CommunicationError is Exception {
    has HTTP::Request $.request;
    has HTTP::Response $.response;

    method message() { "Communication Error" }
}

class GLOBAL::X::Amazon::DynamoDB::Actions::CRCError is Exception {
    has Int $.got-crc32;
    has Int $.expected-crc32;

    method message() { "Response failed CRC32 check, expected $!expected-crc32, but got $!got-crc32" }
}

has Str $.access-key is required;
has Str $.secret-key is required;
has Str $.region is required;

has Str $.scheme = 'https';
has Str $.domain = 'amazonaws.com';

has HTTP::UserAgent $.ua .= new(:useragent<perl6::Amazon::DynamoDB/0>);

method hostname() { "dynamodb.$!region.$!domain" }
method port() { "" }

method make-ddb-request($target, *%request) {
    use HTTP::Request::Common;
    use JSON::Tiny;
    use WebService::AWS::Auth::V4;

    my %cruftless-request = %request.grep({ ?.value });

    my $body = to-json(%cruftless-request);
    my $uri  = "$!scheme://$.hostname$.port/";

    my %headers =
        Host         => $.hostname,
        Content-Type => 'application/x-amz-json-1.0',
        X-Amz-Date   => amz-date-formatter(DateTime.now),
        X-Amz-Target => "DynamoDB_20120810.$target",
        ;

    my Str @headers = %headers.map({ "{.key}:{.value}" });

    my $v4 = WebService::AWS::Auth::V4.new(
        :method<POST>, :$body, :$uri, :@headers, :$!region, :service<dynamodb>,
        :access_key($!access-key), :secret($!secret-key)
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
            die X::Amazon::DynamoDB::Actions::CRCError.new(
                expected-crc32 => $crc32,
                got-crc32      => $got-crc32,
            );
        }

        my %response = from-json($res.decoded-content);
        %response<RequestId> = $request-id;

        return %response;
    }
    else {
        die X::Amazon::DynamoDB::Actions::CommunicationError.new(
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

method Query { ... }
method Scan { ... }
method UpdateItem { ... }

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

method UpdateTable { ... }
method UpdateTimeToLive { ... }

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

method UpdateGlobalTable { ... }

method ListTagsOfResource(
    Str  :$ResourceArn!,

    Str  :$NextToken,
) returns Hash {
    self.make-ddb-request('ListTagsOfResource',
        :$ResourceArn,

        :$NextToken,
    );
}

method TagResource { ... }
method UntagResource { ... }

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

method DescribeBackup { ... }

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

method RestoreTableFromBackup { ... }

method DescribeLimits() returns Hash {
    self.make-ddb-request('DescribeLimits');
}
