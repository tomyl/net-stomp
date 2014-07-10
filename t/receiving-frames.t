use lib 't/lib';
use TestHelp;
use Net::Stomp::Frame;

my ($s,$fh)=mkstomp_testsocket();

subtest 'one frame' => sub {
    my $frame = Net::Stomp::Frame->new({
        command=>'MESSAGE',
        headers=>{'message-id'=>1},
        body=>'string',
    });

    $fh->{to_read}=$frame->as_string;
    my $received = $s->receive_frame;
    cmp_deeply($received,$frame,'received and parsed');
};

subtest 'two frames' => sub {
    my @frames = map {Net::Stomp::Frame->new({
        command=>'MESSAGE',
        headers=>{'message-id'=>$_},
        body=>'string',
    })} (1,2);

    $fh->{to_read}=join '',map {$_->as_string} @frames;
    my $received = $s->receive_frame;
    cmp_deeply($received,$frames[0],'received and parsed');
    $received = $s->receive_frame;
    cmp_deeply($received,$frames[1],'received and parsed');
};

subtest 'a few bytes at a time' => sub {
    my $frame = Net::Stomp::Frame->new({
        command=>'MESSAGE',
        headers=>{'message-id'=>1},
        body=>'string',
    });
    my $frame_string = $frame->as_string;

    $fh->{to_read} = sub {
        return substr($frame_string,0,2,'');
    };
    my $received = $s->receive_frame;
    cmp_deeply($received,$frame,'received and parsed');

};

subtest 'one frame, with content-length' => sub {
    my $str = "string\0with\0zeroes\0";
    my $frame = Net::Stomp::Frame->new({
        command=>'MESSAGE',
        body=>$str,
        headers=>{
            'message-id'=>1,
            'content-length'=>length($str),
        },
    });

    $fh->{to_read}=$frame->as_string;
    my $received = $s->receive_frame;
    cmp_deeply($received,$frame,'received and parsed');
};

subtest 'a few bytes at a time, with content-length' => sub {
    my $str = "string\0with\0zeroes\0";
    my $frame = Net::Stomp::Frame->new({
        command=>'MESSAGE',
        body=>$str,
        headers=>{
            'message-id'=>1,
            'content-length'=>length($str),
        },
    });
    my $frame_string = $frame->as_string;

    $fh->{to_read} = sub {
        return substr($frame_string,0,2,'');
    };
    my $received = $s->receive_frame;
    cmp_deeply($received,$frame,'received and parsed');

};

done_testing;
