use lib 't/lib';
use TestHelp;
use Net::Stomp::Frame;
use Data::Printer;

my ($s,$fh)=mkstomp_testsocket();

my @frames;my $buffer='';
$fh->{written} = sub {
    $buffer .= $_[0];
    my $frame = Net::Stomp::Frame->parse($buffer);
    if ($frame) {
        $buffer='';
        push @frames,$frame;
    }
};

# expected:
# -> BEGIN
# -> SEND
# <- RECEIPT
# -> COMMIT
#
# or
# -> BEGIN
# -> SEND
# <- something else
# -> COMMIT

subtest 'successful' => sub {
    $fh->{to_read} = sub {
        if ($frames[1]) {
            return Net::Stomp::Frame->new({
                command=>'RECEIPT',
                headers=>{'receipt-id'=>$frames[1]->headers->{receipt}},
                body=>undef,
            })->as_string;
        }
        return '';
    };

    $s->send_transactional({some=>'header',body=>'string'});

    is(scalar(@frames),3,'3 frames sent');

    cmp_deeply(
        $frames[0],
        methods(
            command=>'BEGIN',
            headers => {transaction => ignore()},
        ),
        'begin ok',
    ) or note p $frames[0];
    my $transaction = $frames[0]->headers->{transaction};

    cmp_deeply(
        $frames[1],
        methods(
            command => 'SEND',
            headers => {
                some=>'header',
                transaction=>$transaction,
                receipt=>ignore(),
            },
            body => 'string',
        ),
        'send ok',
    ) or note p $frames[1];

    cmp_deeply(
        $frames[2],
        methods(
            command => 'COMMIT',
            headers => {
                transaction=>$transaction,
            },
        ),
        'commit ok',
    ) or note p $frames[2];
};

@frames=();
subtest 'failed' => sub {
    $fh->{to_read} = sub {
        if ($frames[1]) {
            return Net::Stomp::Frame->new({
                command=>'ERROR',
                headers=>{some=>'header'},
                body=>undef,
            })->as_string;
        }
        return '';
    };

    $s->send_transactional({some=>'header',body=>'string'});

    is(scalar(@frames),3,'3 frames sent');

    cmp_deeply(
        $frames[0],
        methods(
            command=>'BEGIN',
            headers => {transaction => ignore()},
        ),
        'begin ok',
    ) or note p $frames[0];
    my $transaction = $frames[0]->headers->{transaction};

    cmp_deeply(
        $frames[1],
        methods(
            command => 'SEND',
            headers => {
                some=>'header',
                transaction=>$transaction,
                receipt=>ignore(),
            },
            body => 'string',
        ),
        'send ok',
    ) or note p $frames[1];

    cmp_deeply(
        $frames[2],
        methods(
            command => 'ABORT',
            headers => {
                transaction=>$transaction,
            },
        ),
        'abort ok',
    ) or note p $frames[2];
};

done_testing;
