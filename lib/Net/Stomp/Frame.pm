package Net::Stomp::Frame;
use strict;
use warnings;
use base 'Class::Accessor::Fast';
__PACKAGE__->mk_accessors(qw(command headers body));

BEGIN {
    for my $header (
        qw(destination exchange content-type content-length message-id))
    {
        my $method = $header;
        $method =~ s/-/_/g;
        no strict 'refs';
        *$method = sub {
            my $self = shift;
            $self->headers->{$header} = shift if @_;
            $self->headers->{$header};
            }
    }
}

sub as_string {
    my $self    = shift;
    my $command = $self->command;
    my $headers = $self->headers;
    my $body    = $self->body;
    my $frame   = $command . "\n";

    # insert a content-length header
    my $bytes_message = 0;
    if ( $headers->{bytes_message} ) {
        $bytes_message = 1;
        delete $headers->{bytes_message};
        $headers->{"content-length"} = length( $self->body );
    }

    while ( my ( $key, $value ) = each %{ $headers || {} } ) {
        $frame .= $key . ':' . $value . "\n";
    }
    $frame .= "\n";
    $frame .= $body || '';
    $frame .= "\000";
}

# NBK - $sock->getline does buffered IO which screws up select.  Use
# sysread one char at a time to avoid reading part of the next line.
sub _readline {
    my($self, $socket, $terminator, $msg) = @_;

    $terminator = "\n" unless defined($terminator);
    $msg ||= "";

    my $s = "";
    while( 1 ) {
        $socket->sysread($s, 1, length($s)) or die("Error reading $msg: $!");
        last if substr($s, -1) eq $terminator;
    }

    return $s;
}

sub parse {
    my ( $package, $socket ) = @_;
    local $/ = "\n";

    # read the command
    my $command;
    while (1) {
        $command = $package->_readline($socket, "\n", "command");
        chop $command;
        last if $command;
    }

    # read headers
    my $headers;
    while (1) {
        my $line = $package->_readline($socket, "\n", "header");
        chop $line;
        last if $line eq "";
        my ( $key, $value ) = split(/: ?/, $line, 2);
        $headers->{$key} = $value;
    }

    # read the body
    my $body;
    my $c;
    if ( $headers->{"content-length"} ) {
        $socket->sysread( $body, $headers->{"content-length"} + 1 )
            || die "Error reading body: $!";
        $headers->{bytes_message} = 1;
    } else {
        $body = $package->_readline($socket, "\000", "body");
    }
    # strip trailing null
    $body =~ s/\000$//;
    
    my $frame = Net::Stomp::Frame->new(
        { command => $command, headers => $headers, body => $body } );

    return $frame;
}

1;

__END__

=head1 NAME

Net::Stomp::Frame - A STOMP Frame

=head1 SYNOPSIS

  use Net::Stomp::Frame;
  my $frame = Net::Stomp::Frame->new( {
    command => $command,
    headers => $headers,
    body    => $body,
  } );
  my $frame  = Net::Stomp::Frame->parse($string);
  my $string = $frame->as_string;
  
=head1 DESCRIPTION

This module encapulates a Stomp frame. Stomp is the Streaming Text
Orientated Messaging Protocol (or the Protocol Briefly Known as TTMP
and Represented by the symbol :ttmp). It's a simple and easy to
implement protocol for working with Message Orientated Middleware from
any language. L<Net::Stomp> is useful for talking to Apache
ActiveMQ, an open source (Apache 2.0 licensed) Java Message Service
1.1 (JMS) message broker packed with many enterprise features.

A Stomp frame consists of a command, a series of headers and a body.

For details on the protocol see L<http://stomp.codehaus.org/Protocol>.

=head1 METHODS

=head2 new

Create a new L<Net::Stomp::Frame> object:

  my $frame = Net::Stomp::Frame->new( {
    command => $command,
    headers => $headers,
    body    => $body,
  } );

=head2 parse

Create a new L<Net::Somp::Frame> given a string containing the serialised frame:

  my $frame  = Net::Stomp::Frame->parse($string);

=head2 as_string

Create a string containing the serialised frame representing the frame:

  my $string = $frame->as_string;

=head2 destination

Get or set the C<destination> header.

=head2 content_type

Get or set the C<content-type> header.

=head2 content_length

Get or set the C<content-length> header.

=head2 exchange

Get or set the C<exchange> header.

=head2 message_id

Get or set the C<message-id> header.

=head1 SEE ALSO

L<Net::Stomp>.

=head1 AUTHOR

Leon Brocard <acme@astray.com>.

=head1 COPYRIGHT

Copyright (C) 2006, Leon Brocard

This module is free software; you can redistribute it or modify it
under the same terms as Perl itself.

