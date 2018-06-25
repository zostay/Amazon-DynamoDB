use v6;

use JSON::Tiny;
use Test;
use Amazon::DynamoDB;

use lib 't/lib';
use Test::Amazon::DynamoDB;

my $rid = 1;
my @reqs;
my $ddb = Amazon::DynamoDB.new(
    scheme => 'https',
    hostname => 'testing',
    port => 1234,
    ua => class :: does Amazon::DynamoDB::UA {
        method request(:$method, :$uri, :%headers, :$content --> Hash) {
            push @reqs, %(
                :$method,
                :$uri,
                :%headers,
                :$content,
            );

            return %(
                Status => 200,
                Header => %(
                    x-amzn-requestid => $rid++,
                    x-amzn-crc32     => 'xxxxxxxxx',
                ),
                RawContent => buf8.new,
                DecodedContent => '{}',
            );
        }
    }.new,
);

{
    my %data =
        RequestItems => %(
            Forum => %(
                Keys => @(
                    %( Name => %(S => 'Amazon DynamoDB') ),
                    %( Name => %(S => 'Amazon RDS') ),
                    %( Name => %(S => 'Amazon Redshift') ),
                ),
                ProjectionExpression => 'Name, Threads, Messages, Views',
            ),
            Thread => %(
                Keys => @(
                    %(
                        ForumName => %( S => 'Amazon DynamoDB' ),
                        Subject => %( S => 'Concurrent reads' ),
                    ),
                ),
                ProjectionExpression => 'Tags, Message',
            ),

        ),
        ReturnConsumedCapacity => 'TOTAL',
    ;

    $ddb.BatchGetItem(|%data);

    is @reqs.elems, 1;

    my $req = @reqs.pop;

    # not testing WebService::AWS::Auth::V4
    $req<headers><Authorization>:delete;
    $req<headers><X-Amz-Date>:delete;

    is $req, %(
        method => 'POST',
        uri    => 'https://testing:1234/',
        headers => %(
            Content-Type => 'application/x-amz-json-1.0',
            Host => 'testing',
            X-Amz-Target => 'DynamoDB_20120810.BatchGetItem',
        ),
        content => to-json(%data),
    );
}

done-testing;
