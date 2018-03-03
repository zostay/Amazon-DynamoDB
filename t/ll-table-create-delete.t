#!/usr/bin/env perl6
use v6;

use Test;
use Amazon::DynamoDB::Actions;

use lib 't/lib';
use Test::Amazon::DynamoDB;

unless test-env-is-ok() {
    plan :skip-all(test-env-skip-message);
}

plan 4;

my $ddb = new-dynamodb-actions();

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
