unit module Test::Amazon::DynamoDB;
use v6;

use Amazon::DynamoDB::Actions;

class Test::Amazon::DynamoDB::Actions is Amazon::DynamoDB::Actions {
    method hostname() { %*ENV<TEST_AWS_DDB_HOSTNAME> }
    method port() {
        %*ENV<TEST_AWS_DDB_PORT>
            ?? ":%*ENV<TEST_AWS_DDB_PORT>"
            !! ""
    }
}

sub new-dynamodb-actions() is export {
    my ($region, $access-key, $secret-key, $scheme)
        = test-env<region access-key secret-key scheme>;
    Test::Amazon::DynamoDB::Actions.new(
        :$region, :$access-key, :$secret-key, :$scheme,
    );
}

sub test-env is export {
    $ //= %(
        scheme       => %*ENV<TEST_AWS_DDB_SCHEME> // 'http',
        hostname     => %*ENV<TEST_AWS_DDB_HOSTNAME>,
        port         => %*ENV<TEST_AWS_DDB_PORT>,
        table-prefix => %*ENV<TEST_AWS_DDB_TABLE_PREFIX>,

        # TODO Support ~/.aws/credentials
        region     => %*ENV<AWS_DEFAULT_REGION>,
        access-key => %*ENV<AWS_ACCESS_KEY_ID>,
        secret-key => %*ENV<AWS_SECRET_ACCESS_KEY>,
    )
}

sub test-env-is-ok is export {
    constant $required = all(<
        hostname region
        access-key secret-key
    >);

    test-env.{ $required }.defined;
}

sub test-env-skip-message is export {
    q<Missing required environment, at least TEST_AWS_DDB_HOSTNAME, TEST_AWS_DDB_TABLE_PREFIX, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY must be set.>;
}

sub test-prefix is export {
    my $test-name = $*PROGRAM-NAME.IO.basename.subst(/.t$/, '');
    $test-name ~ $*PID
}

sub tn(Str $name) is export {
    my $test-tn = join '_', test-prefix(), $name;
    with test-env.<table-prefix> -> $table-prefix {
        join '_', $table-prefix, $test-tn
    }
    else {
        $test-tn
    }
}

sub test-data($name) is export {
    EVALFILE("t/corpus/$name.p6");
}
