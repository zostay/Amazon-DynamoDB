#!/usr/bin/env perl6
use v6;

use Test;
use Amazon::DynamoDB::Actions;

use lib 't/lib';
use Test::Amazon::DynamoDB;

unless test-env-is-ok() {
    plan :skip-all(test-env-skip-message);
}

plan 6;

my $ddb = new-dynamodb-actions();

$ddb.CreateTable(|test-data('Thread-create'));
LAST $ddb.DeleteTable(TableName => tn('Thread'));

lives-ok {
    my $res = $ddb.PutItem(|test-data('Thread-put'));

    # CATCH {
    #     when X::Amazon::DynamoDB::Actions::CommunicationError {
    #         note .request.Str;
    #         note .response.decoded-content;

    #         #.rethrow;
    #     }
    # }

    ok $res;
}

lives-ok {
    my $res = $ddb.GetItem(|test-data('Thread-get'));

    is $res<Item><LastPostDateTime><S>, '201303190422';
    is $res<Item><Message><S>, "I want to update multiple items in a single call. What's the best way to do that?";
    is-deeply set(|$res<Item><Tags><SS>), set("Update","Multiple Items","HelpMe");
}

done-testing;
