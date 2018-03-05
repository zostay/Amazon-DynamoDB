#!/usr/bin/env perl6
use v6;

use Test;
use Amazon::DynamoDB::Actions;

use lib 't/lib';
use Test::Amazon::DynamoDB;

unless test-env-is-ok() {
    plan :skip-all(test-env-skip-message);
}

plan 2;

my $ddb = new-dynamodb-actions();

$ddb.CreateTable(|test-data('Thread-create'));
LAST $ddb.DeleteTable(TableName => tn('Thread'));

lives-ok {
    my $res = $ddb.PutItem(|test-data('Thread-put'));

    CATCH {
        when X::Amazon::DynamoDB::Actions::CommunicationError {
            note .request.Str;
            note .response.decoded-content;

            #.rethrow;
        }
    }

    ok $res;
}

done-testing;
