unit module Test::Amazon::DynamoDB;
use v6;

use Amazon::DynamoDB;
use AWS::Credentials;

my constant $resolver = AWS::Credentials::Provider::FromEnv.new(
    :access-key<TEST_AWS_DDB_ACCESS_KEY_ID>,
    :secret-key<TEST_AWS_DDB_SECRET_ACCESS_KEY>,
    :token<TEST_AWS_DDB_SECURITY_TOKEN TEST_AWS_DDB_SESSION_TOKEN>,
    :expiry-time<TEST_AWS_DDB_CREDENTIAL_EXPIRATION>,
);

sub new-dynamodb-actions() is export {
    my ($scheme, $hostname, $port)
        = test-env<scheme hostname port>;
    my $credentials = load-credentials(:$resolver);
    Amazon::DynamoDB.new(
        :$scheme, :$hostname, :$port, :$credentials,
    );
}

sub test-env is export {
    $ //= %(
        scheme       => %*ENV<TEST_AWS_DDB_SCHEME> // 'http',
        hostname     => %*ENV<TEST_AWS_DDB_HOSTNAME>,
        port         => %*ENV<TEST_AWS_DDB_PORT>.defined ?? %*ENV<TEST_AWS_DDB_PORT>.Int !! Int,
        table-prefix => %*ENV<TEST_AWS_DDB_TABLE_PREFIX>,
    )
}

sub test-env-is-ok is export {
    constant $required = all(< hostname table-prefix >);
    test-env.{ $required }.defined;
}

sub test-env-skip-message is export {
    q<Missing required environment, at least TEST_AWS_DDB_HOSTNAME and TEST_AWS_DDB_TABLE_PREFIX must be set.>;
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
