use v6;

use JSON::Tiny;
use String::CRC32;
use Test;
use Amazon::DynamoDB;

use lib 't/lib';
use Test::Amazon::DynamoDB;

my $rid = 1;
my @reqs;
my @ress;
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

            my $text = to-json(@ress.pop);
            my $blob = $text.encode('UTF-8');

            return %(
                Status => 200,
                Header => %(
                    x-amzn-requestid => $rid++,
                    x-amz-crc32     => String::CRC32::crc32($blob),
                ),
                RawContent => $blob,
                DecodedContent => $text,
            );
        }
    }.new,
);

{
    my %req-data = test-data('AWS-BatchGetItem-Request');
    my %res-data = test-data('AWS-BatchGetItem-Response');
    @ress.push: %res-data;

    my $res = $ddb.BatchGetItem(|%req-data);

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
        content => to-json(%req-data),
    );

    is $res<RequestId>:delete, $rid-1;
    is $res, %res-data;
}

done-testing;
