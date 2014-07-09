package TestHelp;
use strict;
use warnings;
use Net::Stomp;
use Net::Stomp::StupidLogger;

sub mkstomp {
    return Net::Stomp->new({
        logger => Net::Stomp::StupidLogger->new({
            warn => 0, error => 0, fatal => 0,
        }),
        hosts => [ {hostname=>'localhost',port=>61613} ],
        connect_delay => 0,
        @_,
    })
}

sub mkstomp_testsocket {
    my $buffer='';
    open my $fh,'<',\$buffer;
    no warnings 'redefine';
    local *Net::Stomp::_get_socket = sub { return $fh };
    my $s = mkstomp(@_);
    return ($s,$fh);
}

sub import {
    my $caller = caller;
    eval "package $caller; strict->import; warnings->import; use Test::More; use Test::Deep;";
    no strict 'refs';
    *{"${caller}::mkstomp"}=\&mkstomp;
    *{"${caller}::mkstomp_testsocket"}=\&mkstomp_testsocket;
    return;
}

1;
