#!perl
use lib 't/lib';
use TestHelp;

my ($s,$fh) = mkstomp_testsocket;

my $get_socket_called;
{no warnings 'redefine';
*Net::Stomp::_get_socket = sub {
    ++$get_socket_called;
    $fh->{connected}=1;
    return $fh;
}
};
$fh->{to_read} = sub {
    return Net::Stomp::Frame->new({
        command => 'CONNECTED',
        headers => {session=>'foo'},
    })->as_string;
};

sub _testit {
    $get_socket_called=0;
    $s->send({destination=>'here',body=>'string'});
    is($get_socket_called,1,'reconnected ok');
}

subtest 'reconnect on fork' => sub {
    ++$s->{_pid}; # fake a fork
    _testit;
};

subtest 'reconnect on disconnect before send' => sub {
    $fh->{connected}=undef; # fake a disconnect
    _testit;
};

subtest 'reconnect on disconnect while sending' => sub {
    # fake a disconnect after the syswrite, only once
    my $called=0;
    $fh->{written} = sub {
        $fh->{connected} = undef unless $called++;
        return length($_[0]);
    };
    _testit;
};

subtest 'reconnect on write failure' => sub {
    # fake a disconnect after the syswrite, only once
    my $called=0;
    $fh->{written} = sub {
        my $ret;
        if ($called) {
            $ret = $called -1;
        }
        else {
            $ret = undef;
            $!=1;
        }
        ++$called;
        return $ret;
    };
    _testit;
};

done_testing;
