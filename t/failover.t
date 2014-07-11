#!perl
use lib 't/lib';
use TestHelp;

my ($s,$fh) = mkstomp_testsocket(
    hosts => [
        {hostname=>'one',port=>1},
        {hostname=>'two',port=>2},
        {hostname=>'three',port=>3},
    ],
);

my @connected_hosts;
{no warnings 'redefine';
*Net::Stomp::_get_socket = sub {
    my ($self) = @_;
    push @connected_hosts,$self->_cur_host;
    if (@connected_hosts>4) {
        $fh->{connected}=1;
        return $fh;
    }
    else {
        return undef;
    }
}
};
$fh->{to_read} = sub {
    return Net::Stomp::Frame->new({
        command => 'CONNECTED',
        headers => {session=>'foo'},
    })->as_string;
};

$fh->{connected}=undef; # fake a disconnect
$s->send({destination=>'here',body=>'string'});
cmp_deeply(
    \@connected_hosts,
    [1,2,0,1,2],
    'tried all hosts, round-robin, re-starting',
);

done_testing;
