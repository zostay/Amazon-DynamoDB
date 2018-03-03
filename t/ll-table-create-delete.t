#!/usr/bin/env perl6
use v6;

use Test;
use Amazon::DynamoDB::Actions;

my $scheme       = %*ENV<TEST_AWS_DDB_SCHEME> // 'http';
my $hostname     = %*ENV<TEST_AWS_DDB_HOSTNAME>;
my $port         = %*ENV<TEST_AWS_DDB_PORT>;
my $table-prefix = %*ENV<TEST_AWS_DDB_TABLE_PREFIX> // '';

# TODO Support ~/.aws/credentials
my $region     = %*ENV<AWS_DEFAULT_REGION>;
my $access-key = %*ENV<AWS_ACCESS_KEY_ID>;
my $secret-key = %*ENV<AWS_SECRET_ACCESS_KEY>;

unless $hostname && $region && $access-key && $secret-key {
    plan :skip-all<Missing required environment, at least TEST_AWS_DDB_HOSTNAME, TEST_AWS_DDB_TABLE_PREFIX, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY must be set.>;
}

plan 4;

class Test::Amazon::DynamoDB::Actions is Amazon::DynamoDB::Actions {
    method hostname() { %*ENV<TEST_AWS_DDB_HOSTNAME> }
    method port() {
        %*ENV<TEST_AWS_DDB_PORT>
            ?? ":%*ENV<TEST_AWS_DDB_PORT>"
            !! ""
    }
}

sub tn(Str $name) { $table-prefix ?? "{$table-prefix}_$name" !! $name }

my $ddb = Test::Amazon::DynamoDB::Actions.new(
    :$region, :$access-key, :$secret-key, :$scheme,
);

lives-ok {
    my $res = $ddb.CreateTable(
        AttributeDefinitions => [
            {
                AttributeName => 'ForumName',
                AttributeType => 'S',
            },
            {
                AttributeName => 'Subject',
                AttributeType => 'S',
            },
            {
                AttributeName => 'LastPostDateTime',
                AttributeType => 'S',
            },
        ],
        TableName => tn('Thread'),
        KeySchema => [
            {
                AttributeName => 'ForumName',
                KeyType       => 'HASH',
            },
            {
                AttributeName => 'Subject',
                KeyType       => 'RANGE',
            },
        ],
        LocalSecondaryIndexes => [
            {
                IndexName => 'LastPostIndex',
                KeySchema => [
                    {
                        AttributeName => 'ForumName',
                        KeyType       => 'HASH',
                    },
                    {
                        AttributeName => 'LastPostDateTime',
                        KeyType       => 'RANGE',
                    }
                ],
                Projection => {
                    ProjectionType => 'KEYS_ONLY'
                },
            },
        ],
        ProvisionedThroughput => {
            ReadCapacityUnits  => 5,
            WriteCapacityUnits => 5,
        },
    );

    # CATCH {
    #     when X::Amazon::DynamoDB::Actions::CommunicationError {
    #         note .request.Str;
    #         note .response.decoded-content;

    #         #.rethrow;
    #     }
    # }

    is $res<TableDescription><TableName>, tn('Thread');
}

lives-ok {
    my $res = $ddb.DeleteTable(TableName => tn('Thread'));

    is $res<TableDescription><TableName>, tn('Thread');
}

done-testing;
