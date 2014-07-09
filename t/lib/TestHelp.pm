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

sub import {
    my $caller = caller;
    eval "package $caller; use strict; use warnings; use Test::More; use Test::Deep;";
    no strict 'refs';
    *{"${caller}::mkstomp"}=\&mkstomp;
    return;
}

1;
