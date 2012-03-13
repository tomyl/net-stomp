package Net::Stomp;
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Net::Stomp::Frame;
use Carp;
use base 'Class::Accessor::Fast';
our $VERSION = '0.44';

__PACKAGE__->mk_accessors( qw(
    _cur_host failover hostname hosts port select serial session_id socket ssl
    ssl_options subscriptions _connect_headers bufsize
    reconnect_on_fork
) );

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new(@_);

    $self->bufsize(8192) unless $self->bufsize;
    $self->reconnect_on_fork(1) unless defined $self->reconnect_on_fork;

    $self->{_framebuf} = "";

    # We are not subscribed to anything at the start
    $self->subscriptions( {} );

    $self->select( IO::Select->new );
    my @hosts = ();

    # failover://tcp://primary:61616
    # failover:(tcp://primary:61616,tcp://secondary:61616)?randomize=false

    if ($self->failover) {
        my ($uris, $opts) = $self->failover =~ m{^failover:(?://)? \(? (.*?) \)? (?: \? (.*?) ) ?$}ix;

        confess "Unable to parse failover uri: " . $self->failover
                unless $uris;

        foreach my $host (split(/,/,$uris)) {
            $host =~ m{^\w+://([a-zA-Z0-9\-./]+):([0-9]+)$} || confess "Unable to parse failover component: '$host'";
            my ($hostname, $port) = ($1, $2);

            push(@hosts, {hostname => $hostname, port => $port});
        }
    } elsif ($self->hosts) {
        ## @hosts is used inside the while loop later to decide whether we have
        ## cycled through all setup hosts.
        @hosts = @{$self->hosts};
    }
    $self->hosts(@hosts);

    my $err;
    {
        local $@ = 'run me!';
        while($@) {
            eval { $self->_get_connection };
            last unless $@;
            if (!@hosts || $self->_cur_host == $#hosts ) {
                # We've cycled through all setup hosts. Die now. Can't die because
                # $@ is localized.
                $err = $@;
                last;
            }
            sleep(5);
        }
    }
    die $err if $err;
    return $self;
}

sub _get_connection {
    my $self = shift;
    if (my $hosts = $self->hosts) {
        if (defined $self->_cur_host && ($self->_cur_host < $#{$hosts} ) ) {
            $self->_cur_host($self->_cur_host+1);
        } else {
            $self->_cur_host(0);
        }
        $self->hostname($hosts->[$self->_cur_host]->{hostname});
        $self->port($hosts->[$self->_cur_host]->{port});
    }
    my ($socket);
    my %sockopts = (
        PeerAddr => $self->hostname,
        PeerPort => $self->port,
        Proto    => 'tcp',
        Timeout  => 5
    );
    if ( $self->ssl ) {
        eval { require IO::Socket::SSL };
        die
            "You should install the IO::Socket::SSL module for SSL support in Net::Stomp"
            if $@;
        %sockopts = ( %sockopts, %{ $self->ssl_options || {} } );
        $socket = IO::Socket::SSL->new(%sockopts);
    } else {
        $socket = IO::Socket::INET->new(%sockopts);
        binmode($socket) if $socket;
    }
    die "Error connecting to " . $self->hostname . ':' . $self->port . ": $@"
        unless $socket;

    $self->select->remove($self->socket) if $self->socket;

    $self->select->add($socket);
    $self->socket($socket);
    $self->{_pid} = $$;
}

sub connect {
    my ( $self, $conf ) = @_;

    my $frame = Net::Stomp::Frame->new(
        { command => 'CONNECT', headers => $conf } );
    $self->send_frame($frame);
    $frame = $self->receive_frame;

    # Setting initial values for session id, as given from
    # the stomp server
    $self->session_id( $frame->headers->{session} );
    $self->_connect_headers( $conf );

    return $frame;
}

sub disconnect {
    my $self = shift;
    my $frame = Net::Stomp::Frame->new( { command => 'DISCONNECT' } );
    $self->send_frame($frame);
    $self->socket->close;
    $self->select->remove($self->socket);
}

sub _reconnect {
    my $self = shift;
    if ($self->socket) {
        $self->socket->close;
    }
    eval { $self->_get_connection };
    while ($@) {
        sleep(5);
        eval { $self->_get_connection };
    }
    $self->connect( $self->_connect_headers );
    for my $sub(keys %{$self->subscriptions}) {
        $self->subscribe($self->subscriptions->{$sub});
    }
}

sub can_read {
    my ( $self, $conf ) = @_;

    # If there is any data left in the framebuffer that we haven't read, return
    # 'true'. But we don't want to spin endlessly, so only return true the
    # first time. (Anything touching the _framebuf should update this flag when
    # it does something.
    if ( $self->{_framebuf_changed} && length $self->{_framebuf} ) {
        $self->{_framebuf_changed} = 0;
        return 1;
    }

    $conf ||= {};
    my $timeout = exists $conf->{timeout} ? $conf->{timeout} : undef;
    return $self->select->can_read($timeout) || 0;
}

sub send {
    my ( $self, $conf ) = @_;
    my $body = $conf->{body};
    delete $conf->{body};
    my $frame = Net::Stomp::Frame->new(
        { command => 'SEND', headers => $conf, body => $body } );
    $self->send_frame($frame);
}

sub send_transactional {
    my ( $self, $conf ) = @_;
    my $body = $conf->{body};
    delete $conf->{body};

    # begin the transaction
    my $transaction_id = $self->_get_next_transaction;
    my $begin_frame
        = Net::Stomp::Frame->new(
        { command => 'BEGIN', headers => { transaction => $transaction_id } }
        );
    $self->send_frame($begin_frame);

    # send the message
    my $receipt_id = $self->_get_next_transaction;
    $conf->{receipt} = $receipt_id;
    my $message_frame = Net::Stomp::Frame->new(
        { command => 'SEND', headers => $conf, body => $body } );
    $self->send_frame($message_frame);

    # check the receipt
    my $receipt_frame = $self->receive_frame;
    if (   $receipt_frame->command eq 'RECEIPT'
        && $receipt_frame->headers->{'receipt-id'} eq $receipt_id )
    {

        # success, commit the transaction
        my $frame_commit = Net::Stomp::Frame->new(
            {   command => 'COMMIT',
                headers => { transaction => $transaction_id }
            }
        );
        return $self->send_frame($frame_commit);
    } else {

        # some failure, abort transaction
        my $frame_abort = Net::Stomp::Frame->new(
            {   command => 'ABORT',
                headers => { transaction => $transaction_id }
            }
        );
        $self->send_frame($frame_abort);
        return 0;
    }
}

sub _sub_key {
    my ($conf) = @_;

    if ($conf->{id}) { return "id-".$conf->{id} }
    return "dest-".$conf->{destination}
}

sub subscribe {
    my ( $self, $conf ) = @_;
    my $frame = Net::Stomp::Frame->new(
        { command => 'SUBSCRIBE', headers => $conf } );
    $self->send_frame($frame);
    my $subs = $self->subscriptions;
    $subs->{_sub_key($conf)} = $conf;
}

sub unsubscribe {
    my ( $self, $conf ) = @_;
    my $frame = Net::Stomp::Frame->new(
        { command => 'UNSUBSCRIBE', headers => $conf } );
    $self->send_frame($frame);
    my $subs = $self->subscriptions;
    delete $subs->{_sub_key($conf)}
}

sub ack {
    my ( $self, $conf ) = @_;
    my $id    = $conf->{frame}->headers->{'message-id'};
    my $frame = Net::Stomp::Frame->new(
        { command => 'ACK', headers => { 'message-id' => $id } } );
    $self->send_frame($frame);
}

sub send_frame {
    my ( $self, $frame ) = @_;
    # see if we're connected before we try to syswrite()
    if (not defined $self->_connected) {
        $self->_reconnect;
        if (not defined $self->_connected) {
            warn q{wasn't connected; couldn't _reconnect()};
        }
    }
    my $written = $self->socket->syswrite( $frame->as_string );
    if (($written||0) != length($frame->as_string)) {
        warn 'only wrote '
            . ($written||0)
            . ' characters out of the '
            . length($frame->as_string)
            . ' character frame';
        warn 'problem frame: <<' . $frame->as_string . '>>';
    }
    unless (defined $self->_connected) {
        $self->_reconnect;
        $self->send_frame($frame);
    }
}

sub _read_data {
    my ($self, $timeout) = @_;

    return unless $self->select->can_read($timeout);
    my $len = $self->socket->sysread($self->{_framebuf},
                                     $self->bufsize,
                                     length($self->{_framebuf} || ''));

    if ($len && $len > 0) {
        $self->{_framebuf_changed} = 1;
    }
    else {
        # EOF detected - connection is gone. We have to reset the framebuf in
        # case we had a partial frame in there that will never arrive.
        $self->{_framebuf} = "";
        delete $self->{_command};
        delete $self->{_headers};
    }
    return $len;
}

sub _read_headers {
    my ($self) = @_;

    if ($self->{_framebuf} =~ s/^\n*([^\n].*?)\n\n//s) {
        $self->{_framebuf_changed} = 1;
        my $raw_headers = $1;
        if ($raw_headers =~ s/^(.+)\n//) {
            $self->{_command} = $1;
        }
        foreach my $line (split(/\n/, $raw_headers)) {
            my ($key, $value) = split(/\s*:\s*/, $line, 2);
            $self->{_headers}->{$key} = $value;
        }
        return 1;
    }
    return 0;
}

sub _read_body {
    my ($self) = @_;

    my $h = $self->{_headers};
    if ($h->{'content-length'}) {
        if (length($self->{_framebuf}) >= $h->{'content-length'}) {
            $self->{_framebuf_changed} = 1;
            my $body = substr($self->{_framebuf},
                              0,
                              $h->{'content-length'},
                              '' );

            # Trim the trailer off the frame.
            $self->{_framebuf} =~ s/^.*?\000\n*//s;
            return Net::Stomp::Frame->new({
                command => delete $self->{_command},
                headers => delete $self->{_headers},
                body => $body
            });
        }
    } elsif ($self->{_framebuf} =~ s/^(.*?)\000\n*//s) {
        # No content-length header.

        my $body = $1;
        $self->{_framebuf_changed} = 1;
        return Net::Stomp::Frame->new({
              command => delete $self->{_command},
              headers => delete $self->{_headers},
              body => $body });
    }

    return 0;
}

# this method is to stop the pointless warnings being thrown when trying to
# call peername() on a closed socket, i.e.
#   getpeername() on closed socket GEN125 at
#   /opt/xt/xt-perl/lib/5.12.3/x86_64-linux/IO/Socket.pm line 258.
#
# solution taken from:
# http://objectmix.com/perl/80545-warning-getpeername.html
sub _connected {
    my $self = shift;

    return if $self->{_pid} != $$ and $self->reconnect_on_fork;

    my $connected;
    {
        local $^W = 0;
        $connected = $self->socket->connected;
    }
    return $connected;
}

sub receive_frame {
    my ($self, $conf) = @_;

    my $timeout = exists $conf->{timeout} ? $conf->{timeout} : undef;

    unless (defined $self->_connected) {
        $self->_reconnect;
    }

    my $done = 0;
    while ( not $done = $self->_read_headers ) {
        return undef unless $self->_read_data($timeout);
    }
    while ( not $done = $self->_read_body ) {
        return undef unless $self->_read_data($timeout);
    }

    return $done;
}

sub _get_next_transaction {
    my $self = shift;
    my $serial = $self->serial || 0;
    $serial++;
    $self->serial($serial);

    return ($self->session_id||'nosession') . '-' . $serial;
}

1;

__END__

=head1 NAME

Net::Stomp - A Streaming Text Orientated Messaging Protocol Client

=head1 SYNOPSIS

  # send a message to the queue 'foo'
  use Net::Stomp;
  my $stomp = Net::Stomp->new( { hostname => 'localhost', port => '61613' } );
  $stomp->connect( { login => 'hello', passcode => 'there' } );
  $stomp->send(
      { destination => '/queue/foo', body => 'test message' } );
  $stomp->disconnect;

  # subscribe to messages from the queue 'foo'
  use Net::Stomp;
  my $stomp = Net::Stomp->new( { hostname => 'localhost', port => '61613' } );
  $stomp->connect( { login => 'hello', passcode => 'there' } );
  $stomp->subscribe(
      {   destination             => '/queue/foo',
          'ack'                   => 'client',
          'activemq.prefetchSize' => 1
      }
  );
  while (1) {
    my $frame = $stomp->receive_frame;
    warn $frame->body; # do something here
    $stomp->ack( { frame => $frame } );
  }
  $stomp->disconnect;

  # write your own frame
  my $frame = Net::Stomp::Frame->new(
       { command => $command, headers => $conf, body => $body } );
  $self->send_frame($frame);

  # connect with failover supporting similar URI to ActiveMQ
  $stomp = Net::Stomp->new({ failover => "failover://tcp://primary:61616" })
  # "?randomize=..." and other parameters are ignored currently
  $stomp = Net::Stomp->new({ failover => "failover:(tcp://primary:61616,tcp://secondary:61616)?randomize=false" })

  # Or in a more natural perl way
  $stomp = Net::Stomp->new({ hosts => [
    { hostname => 'primary', port => 61616 },
    { hostname => 'secondary', port => 61616 },
  ] });

=head1 DESCRIPTION

This module allows you to write a Stomp client. Stomp is the Streaming
Text Orientated Messaging Protocol (or the Protocol Briefly Known as
TTMP and Represented by the symbol :ttmp). It's a simple and easy to
implement protocol for working with Message Orientated Middleware from
any language. L<Net::Stomp> is useful for talking to Apache ActiveMQ,
an open source (Apache 2.0 licensed) Java Message Service 1.1 (JMS)
message broker packed with many enterprise features.

A Stomp frame consists of a command, a series of headers and a body -
see L<Net::Stomp::Frame> for more details.

For details on the protocol see L<http://stomp.codehaus.org/Protocol>.

To enable the ActiveMQ Broker for Stomp add the following to the
activemq.xml configuration inside the <transportConnectors> section:

  <transportConnector name="stomp" uri="stomp://localhost:61613"/>

To enable the ActiveMQ Broker for Stomp and SSL add the following
inside the <transportConnectors> section:

  <transportConnector name="stomp+ssl" uri="stomp+ssl://localhost:61612"/>

For details on Stomp in ActiveMQ See L<http://activemq.apache.org/stomp.html>.

=head1 METHODS

=head2 new

The constructor creates a new object. You must pass in a hostname and
a port or set a failover configuration:

  my $stomp = Net::Stomp->new( { hostname => 'localhost', port => '61613' } );

If you want to use SSL, make sure you have L<IO::Socket::SSL> and
pass in the SSL flag:

  my $stomp = Net::Stomp->new( {
    hostname => 'localhost',
    port     => '61612',
    ssl      => 1,
  } );

If you want to pass in L<IO::Socket::SSL> options:

  my $stomp = Net::Stomp->new( {
    hostname    => 'localhost',
    port        => '61612',
    ssl         => 1,
    ssl_options => { SSL_cipher_list => 'ALL:!EXPORT' },
  } );

=head3 Failover

There is experiemental failover support in Net::Stomp. You can specify failover
in a similar maner to ActiveMQ
(L<http://activemq.apache.org/failover-transport-reference.html>) for
similarity with Java configs or using a more natural method to perl of passing
in an array-of-hashrefs in the C<hosts> parameter.

Currently when ever Net::Stomp connects or reconnects it will simply try the
next host in the list.

=head3 Reconnect on C<fork>

By default Net::Stomp will reconnect, using a different socket, if the
process C<fork>s. This avoids problems when parent & child write to
the socket at the same time. If, for whatever reason, you don't want
this to happen, set C<reconnect_on_fork> to C<0> (either as a
constructor parameter, or by calling the method).

=head2 connect

This connects to the Stomp server. You may pass in a C<login> and
C<passcode> options.

You may also pass in 'client-id', which specifies the JMS Client ID which is
used in combination to the activemqq.subscriptionName to denote a durable
subscriber.

  $stomp->connect( { login => 'hello', passcode => 'there' } );

=head2 send

This sends a message to a queue or topic. You must pass in a destination and a
body.

  $stomp->send(
      { destination => '/queue/foo', body => 'test message' } );

To send a BytesMessage, you should set the field 'bytes_message' to 1.

=head2 send_transactional

This sends a message in transactional mode and fails if the receipt of the
message is not acknowledged by the server:

  $stomp->send_transactional(
      { destination => '/queue/foo', body => 'test message' }
  ) or die "Couldn't send the message!";

If using ActiveMQ, you might also want to make the message persistent:

  $stomp->send_transactional(
      { destination => '/queue/foo', body => 'test message', persistent => 'true' }
  ) or die "Couldn't send the message!";

=head2 disconnect

This disconnects from the Stomp server:

  $stomp->disconnect;

=head2 subscribe

This subscribes you to a queue or topic. You must pass in a destination.

The acknowledge mode defaults to 'auto', which means that frames will
be considered delivered after they have been sent to a client. The
other option is 'client', which means that messages will only be
considered delivered after the client specifically acknowledges them
with an ACK frame.

Other options:

'selector': which specifies a JMS Selector using SQL
92 syntax as specified in the JMS 1.1 specificiation. This allows a
filter to be applied to each message as part of the subscription.

'activemq.dispatchAsync': should messages be dispatched synchronously
or asynchronously from the producer thread for non-durable topics in
the broker. For fast consumers set this to false. For slow consumers
set it to true so that dispatching will not block fast consumers.

'activemq.exclusive': Would I like to be an Exclusive Consumer on a queue.

'activemq.maximumPendingMessageLimit': For Slow Consumer Handlingon
non-durable topics by dropping old messages - we can set a maximum
pending limit which once a slow consumer backs up to this high water
mark we begin to discard old messages.

'activemq.noLocal': Specifies whether or not locally sent messages
should be ignored for subscriptions. Set to true to filter out locally
sent messages.

'activemq.prefetchSize': Specifies the maximum number of pending
messages that will be dispatched to the client. Once this maximum is
reached no more messages are dispatched until the client acknowledges
a message. Set to 1 for very fair distribution of messages across
consumers where processing messages can be slow.

'activemq.priority': Sets the priority of the consumer so that
dispatching can be weighted in priority order.

'activemq.retroactive': For non-durable topics do you wish this
subscription to the retroactive.

'activemq.subscriptionName': For durable topic subscriptions you must
specify the same clientId on the connection and subscriberName on the
subscribe.

  $stomp->subscribe(
      {   destination             => '/queue/foo',
          'ack'                   => 'client',
          'activemq.prefetchSize' => 1
      }
  );

=head2 unsubscribe

This unsubscribes you to a queue or topic. You must pass in a destination:

  $stomp->unsubcribe({ destination => '/queue/foo' });

=head2 receive_frame

This blocks and returns you the next Stomp frame.

  my $frame = $stomp->receive_frame;
  warn $frame->body; # do something here

The header bytes_message is 1 if the message was a BytesMessage.

By default this method will block until a frame can be returned. If you wish to
wait for a specified time pass a C<timeout> argument:

  # Wait half a second for a frame, else return undef
  $stomp->receive_frame({ timeout => 0.5 })

=head2 can_read

This returns whether there is new data is waiting to be read from the STOMP
server. Optionally takes a timeout in seconds:

  my $can_read = $stomp->can_read;
  my $can_read = $stomp->can_read({ timeout => '0.1' });

C<undef> says block until something can be read, C<0> says to poll and return
immediately.

=head2 ack

This acknowledges that you have received and processed a frame (if you
are using client acknowledgements):

  $stomp->ack( { frame => $frame } );

=head2 send_frame

If this module does not provide enough help for sending frames, you
may construct your own frame and send it:

  # write your own frame
  my $frame = Net::Stomp::Frame->new(
       { command => $command, headers => $conf, body => $body } );
  $self->send_frame($frame);

=head1 SEE ALSO

L<Net::Stomp::Frame>.

=head1 AUTHORS

Leon Brocard <acme@astray.com>,
Thom May <thom.may@betfair.com>,
Michael S. Fischer <michael@dynamine.net>,
Ash Berlin <ash_github@firemirror.com>

=head1 CONTRIBUTORS

Paul Driver <frodwith@cpan.org>,
Andreas Faafeng <aff@cpan.org>,
Vigith Maurice <vigith@yahoo-inc.com>,
Stephen Fralich <sjf4@uw.edu>,
Squeeks <squeek@cpan.org>,
Chisel Wright <chisel@chizography.net>,

=head1 COPYRIGHT

Copyright (C) 2006-9, Leon Brocard
Copyright (C) 2009, Thom May, Betfair.com
Copyright (C) 2010, Ash Berlin, Net-a-Porter.com
Copyright (C) 2010, Michael S. Fischer

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

