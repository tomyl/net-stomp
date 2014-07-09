#!perl
use lib 't/lib';
use TestHelp;
use Test::Fatal;

our @sockets;
{no warnings 'redefine';
 sub Net::Stomp::_get_socket { return shift @sockets }
}

subtest 'simplest case' => sub {
    local @sockets=(\*STDIN);
    my $s = mkstomp();
    cmp_deeply(
        $s,
        methods(
            hostname => 'localhost',
            port => 61613,
            _cur_host => 0,
            socket => \*STDIN,
            select => noclass(superhashof({socket=>\*STDIN})),
        ),
        'correct',
    );
};

subtest 'simplest case, old style' => sub {
    local @sockets=(\*STDIN);
    my $s = mkstomp(hosts=>undef,hostname=>'localhost',port=>61613,);
    cmp_deeply(
        $s,
        methods(
            hostname => 'localhost',
            port => 61613,
            _cur_host => undef,
            socket => \*STDIN,
            select => noclass(superhashof({socket=>\*STDIN})),
        ),
        'correct',
    );
};

subtest 'two host, first one' => sub {
    local @sockets=(\*STDIN);
    my $s = mkstomp(hosts=>[{hostname=>'one',port=>1234},{hostname=>'two',port=>3456}]);
    cmp_deeply(
        $s,
        methods(
            hostname => 'one',
            port => 1234,
            _cur_host => 0,
            socket => \*STDIN,
        ),
        'correct',
    );
};

subtest 'two host, second one' => sub {
    local @sockets=(undef,\*STDIN);
    my $s = mkstomp(hosts=>[{hostname=>'one',port=>1234},{hostname=>'two',port=>3456}]);
    cmp_deeply(
        $s,
        methods(
            hostname => 'two',
            port => 3456,
            _cur_host => 1,
            socket => \*STDIN,
        ),
        'correct',
    );
};

subtest 'two host, none' => sub {
    local @sockets=(undef,undef);
    my $s;
    my $err = exception { $s=mkstomp(hosts=>[{hostname=>'one',port=>1234},{hostname=>'two',port=>3456}]) };
    cmp_deeply($s,undef,'expected failure');
    cmp_deeply($err,re(qr{Error connecting}),'expected exception');
};

subtest 'old style, failure' => sub {
    local @sockets=(undef);
    my $s;
    my $err = exception { $s=mkstomp(hosts=>undef,hostname=>'localhost',port=>61613,) };
    cmp_deeply($s,undef,'expected failure');
    cmp_deeply($err,re(qr{Error connecting}),'expected exception');
};

done_testing;
