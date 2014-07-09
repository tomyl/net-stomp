{package TestHelp;
use strict;
use warnings;
BEGIN { $INC{'IO/Select.pm'}=__FILE__ }
use Net::Stomp;

sub mkstomp {
    return Net::Stomp->new({
        logger => TestHelp::Logger->new(),
        hosts => [ {hostname=>'localhost',port=>61613} ],
        connect_delay => 0,
        @_,
    })
}

sub mkstomp_testsocket {
    my $fh = TestHelp::Socket->new({
        connected=>1,
        buffer=>'',
    });
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
}

{package TestHelp::Socket;
use strict;
use warnings;

sub new {
    bless $_[1],$_[0];
}
sub connected { return $_[0]->{connected} }
sub close { }
sub syswrite { }

sub sysread {
    my ($self,$dest,$length,$offset) = @_;

    my $string = ref($self->{buffer})?($self->{buffer}->()):($self->{buffer});

    my $ret = substr($string,0,$length,'');
    substr($_[1],$offset) = $ret;
    return length $ret;
}
}

{package IO::Select;
use strict;
use warnings;

sub new { bless {},$_[0] }

sub add { $_[0]->{socket}=$_[1] }
sub remove { delete $_[0]->{socket} }

sub can_read { return $_[0]->{socket} && $_[0]->{socket}{buffer} ne '' }
}

{package TestHelp::Logger;
use strict;
use warnings;
use base 'Net::Stomp::StupidLogger';

sub _log {
    my ($self,$level,@etc) = @_;
    Test::More::note("log $level: @etc");
}
}

1;
