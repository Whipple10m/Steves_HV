#!/usr/bin/perl -w

use strict;

use FileHandle;
use IO::Socket;

#
# ************************** Package HV::Constant ***************************
#

package HV::Constant;
use FileHandle;
use Config;
use Exporter();

BEGIN
  {
    @HV::Constant::ISA = qw(Exporter);

    @HV::Constant::EXPORT=
      qw(HV_MAINFRAME_PORT HV_SLOTS_PER_MODULE HV_MEASURE_GOOD_TIME 
	 HV_FAKE_IT HV_MOST_NEGATIVE_VOLTAGE
	 HV_MODULES_PER_CRATE $HV_RunningThreaded
	 COMMUNICATE_DEMAND COMMUNICATE_ENABLE
	 &HV_verbosity $DEBUG_STREAM);

    use constant HV_MAINFRAME_PORT         => 1090;
    use constant HV_SLOTS_PER_MODULE       => 12;
    use constant HV_MODULES_PER_CRATE      => 16;
    use constant HV_MEASURE_GOOD_TIME      => 20;
    use constant HV_MOST_NEGATIVE_VOLTAGE  => -1400;
    use constant HV_FAKE_IT                => 1;

    use constant COMMUNICATE_DEMAND => 1;
    use constant COMMUNICATE_ENABLE => 2;
  }

BEGIN
  {
    $HV::Constant::HV_RunningThreaded=0;
    $HV::Constant::HV_RunningThreaded=1 if ( $Config{'usethreads'} );
  }

$HV::Constant::DEBUG_STREAM=new HV::Debug;
$HV::Constant::DEBUG_STREAM->fdopen(*STDERR,"w");

$HV::Constant::Verbosity=0;
sub HV_verbosity
  {
    my $verb=shift;
    $HV::Constant::Verbosity=$verb if(defined $verb);
    return $HV::Constant::Verbosity;
  }


#
# **************************** Package HV::Debug ****************************
#

#
# This packages only purpose is to serialise outputs to the STDERR which the
# threads use to write messages. All that is here is a simple wrapper on the
# functions provided by the FileHandle package. The "new" function just 
# passes an object back that has a FileHandle in it. All other methods are
# handled by the "AUTOLOAD" function which locks the file handle for use by
# this thread (if we are thread-enabled) and then passes the request to the 
# filehandle itself, so HV::Debug->print is really FileHandle->print etc..
#

package HV::Debug;
BEGIN { HV::Constant->import; }
BEGIN { if ( $HV_RunningThreaded ) { require Thread; import Thread; } }

sub new
  {
    my $pkg=shift;

    my $this = { "fh"    => undef };
    bless $this,$pkg;

    $this->{"fh"} = new FileHandle @_;

    return $this;
  }

sub AUTOLOAD
  {
    no strict 'vars';
    my $this=shift;
    my $name=$AUTOLOAD;
    $name =~ s/.*://;

    return if $name eq "DESTROY";

    lock $this->{"fh"} if ( $HV_RunningThreaded );

    return $this->{"fh"}->$name(@_);
  }

#
# **************************** Package HV::Slot *****************************
#

#
# This package contains the computer representation of an individual HV
# channel. Each HV::Slot belongs to a HV::Module which in turn resides in
# a HV::Crate. The "module" method provides a back reference to "our" module.
# We can "demand" voltages, "enable" the channel, read and write the 
# settings to the crate (via the HV::Crate object) and also print out the 
# current settings.
#

package HV::Slot;
BEGIN { HV::Constant->import; }
BEGIN { if ( $HV_RunningThreaded ) { require Thread; import Thread; } }

sub new
  {
    my $pkg=shift;
    my $module=shift;
    my $slotnum=shift;

    die "Cannot clone Slot !" if(ref($pkg));

    my $this = { "module"        => $module,
		 "slotnum"       => $slotnum,
		 "demandvoltage" => 0,
		 "enabled"       => 0,
	       };

    bless $this,$pkg;

    return $this;
  }

sub module
  {
    my $this=shift;
    return $this->{"module"};
  }

sub slotnum
  {
    my $this=shift;
    return $this->{"slotnum"};
  }

sub enable
  {
    my $this=shift;
    my $enable=shift;
    my $immediatewrite=shift;
    if((defined $enable) and ($this->{"enabled"} != $enable))
      {
	$this->{"enabled"}=$enable;
	module($this)->seated(1);
	write_setting($this,COMMUNICATE_ENABLE) if(defined $immediatewrite);
      }
    return $this->{"enabled"};
  }

sub enable_and_write
  {
    my $this=shift;
    my $enable=shift;
    return enable($this,$enable,1);
  }

sub demand
  {
    my $this=shift;
    my $demandvoltage=shift;
    my $immediatewrite=shift;
    if(defined $demandvoltage)
      {
	if($demandvoltage>0)
	  {
	    $DEBUG_STREAM->print("Request to set voltage to POSITIVE voltage ",
				 $demandvoltage,"V ignored!\n");
	  }
	elsif($demandvoltage<HV_MOST_NEGATIVE_VOLTAGE)
	  {
	    $DEBUG_STREAM->print("Request to set voltage to ",
				 $demandvoltage,"V (ie. less than ",
				 HV_MOST_NEGATIVE_VOLTAGE,
				 "V) ignored!\n");
	  }
	elsif($this->{"demandvoltage"}!=$demandvoltage)
	  {
	    $this->{"demandvoltage"}=$demandvoltage;
	    module($this)->seated(1);
	    write_setting($this,COMMUNICATE_DEMAND) 
	      if(defined $immediatewrite);
	  }
      }
    return $this->{"demandvoltage"};
  }

sub demand_and_write
  {
    my $this=shift;
    my $demandvoltage=shift;
    return demand($this,$demandvoltage,1);
  }

sub update
  {
    my $this=shift;
    my $updates=shift;

    demand($this,$updates->{"demand"},1)
      if(exists $updates->{"demand"});

    enable($this,$updates->{"enable"},1)
      if(exists $updates->{"enable"});
}

sub set_measures
  {
    my $this=shift;
    my $measures=shift;
    $this->{"measure_time"} = time if(defined $measures);
    $this->{"measures"} = $measures if(defined $measures);
    enable($this,$measures->{"ce"}) if(exists $measures->{"ce"});
  }

sub measure
  {
    my $this=shift;

    if((defined $this->{"measure_time"}) and
       ((time - $this->{"measure_time"}) < HV_MEASURE_GOOD_TIME))
      {
	my $measures=$this->{"measures"};
	undef $this->{"measures"};
	undef $this->{"measure_time"};
	return $measures;
      }
    else
      {
	my %measures;
	my @tomeasure=grep { $_ } @_;
	@tomeasure= qw(CE DV MV MC ST) if ( scalar(@tomeasure) == 0 );

	foreach ( @tomeasure )
	  {
	    my @response=send_command($this,"RC",$_);
	    if((lc $response[0] ne "rc") or
	       (lc $response[1] ne lc S($this)) or
	       (lc $response[2] ne lc $_))
	      {
		$DEBUG_STREAM->print("Invalid response from crate:",
				     join(" ",@response),"\n");
	      }
	    else
	      {
		$measures{lc $_}=$response[3];
	      }
	  }
	enable($this,$measures{"ce"}) if(exists $measures{"ce"});
	
	return \%measures;
      }
  }

sub slot_status
  {
    my $statusword=shift;

    return unless ( defined $statusword );

    my $statusbitstring=unpack("B16",pack("H4",$statusword));
    my @statusbit=reverse $statusbitstring =~ /(.)/g;

    my @response;
    push @response,"* RAMPING - UP *"            if ( $statusbit[1] );
    push @response,"* RAMPING - DOWN *"          if ( $statusbit[2] );
    push @response,"* TRIPPED - SUPPLY LIMIT *"  if ( $statusbit[4] );
    push @response,"* TRIPPED - CURRENT LIMIT *" if ( $statusbit[5] );
    push @response,"* TRIPPED - VOLTAGE ERROR *" if ( $statusbit[6] );
    push @response,"* TRIPPED - VOLTAGE LIMIT *" if ( $statusbit[7] );

    return { "enabled" => $statusbit[0],
	     "ramping" => ($statusbit[1] or $statusbit[2]),
	     "tripped" => ($statusbit[4] or $statusbit[5] or
			   $statusbit[6] or $statusbit[7]),
	     "text"    => join(" ",@response) };
  }

sub display
  {
    my $this=shift;
    my $fp=shift;

    if(enable($this))
      {
	$fp->printf("%5d",demand($this));
      }
    else
      {
	$fp->print("*****");
      }
  }

sub S
  {
    my $this=shift;
    return sprintf("S%1d.%1d",module($this)->modnum,slotnum($this));
  }

sub zero
  {
    my $this=shift;
    demand($this,0);
    enable($this,0);
  }

sub send_command
  {
    my $this=shift;
    my $cmd_action=shift;
    my $cmd_option=shift;

    return unless(module($this)->seated);

    my $full_command=join(" ",$cmd_action,S($this),$cmd_option);

    return module($this)->crate->send_command($full_command);
  }

sub write_setting
  {
    my $this=shift;
    my $writemask=shift;

    return unless(module($this)->seated);

    if((not defined $writemask) or ($writemask & COMMUNICATE_ENABLE))
      {
	my $enable="CE ".sprintf("%1d",enable($this));
	send_command($this,"LD",$enable);
      }

    if((not defined $writemask) or ($writemask & COMMUNICATE_DEMAND))
      {
	my $demand="DV ".sprintf("%.1f",demand($this));
	send_command($this,"LD",$demand);
      }

    return;
  }

sub read_setting
  {
    my $this=shift;

    return unless(module($this)->seated);

    my @enables=send_command($this,"RC","CE");
    if((lc $enables[0] ne "rc") or
       (lc $enables[1] ne lc S($this)) or
       (lc $enables[2] ne "ce"))
      {
	$DEBUG_STREAM->print("Invalid response from crate:",
			     join(" ",@enables),"\n");
	die;
      }

    my @demands=send_command($this,"RC","DV");
    if((lc shift(@demands) ne "rc") or
       (lc shift(@demands) ne lc S($this)) or
       (lc shift(@demands) ne "dv"))
      {
	$DEBUG_STREAM->print("Invalid response from crate:",
			     join(" ",@demands),"\n");
	die;
      }

    enable($this,$enables[4]);
    demand($this,$demands[4]);

    return;
  }

#
# **************************** Package HV::Module ***************************
#

#
# A HV::Module is a representation of the physical slot of a HV::Crate. There
# need not have be module physically in the crate slot though. This module is
# a container for HV::Slot's. It also provides some handy routines to read
# and write to a whole set of slots in one network command.
#

package HV::Module;
BEGIN { HV::Constant->import; }
BEGIN { if ( $HV_RunningThreaded ) { require Thread; import Thread; } }

sub new
  {
    my $pkg=shift;
    my $crate=shift;
    my $modnum=shift;

    die "Cannot clone Module !" if(ref($pkg));

    my $this = { "crate"  => $crate,
		 "modnum" => $modnum,
		 "seated" => 0,
	       };
    bless $this,$pkg;

    my $slot;
    for($slot=0;$slot<HV_SLOTS_PER_MODULE;$slot++)
      {
	$this->{"slot"}->[$slot]=new HV::Slot $this,$slot;
      }

    return $this;
  }

sub crate
  {
    my $this=shift;
    return $this->{"crate"};
  }

sub modnum
  {
    my $this=shift;
    return $this->{"modnum"};
  }

sub slot
  {
    my $this=shift;
    my $slot=shift;

    return $this->{"slot"}->[$slot] if ((defined $slot) &&
					(($slot>=0) &&
					 ($slot<HV_SLOTS_PER_MODULE)));

    return undef;
  }

sub seated
  {
    my $this=shift;
    my $seated=shift;
    $this->{"seated"}=$seated if defined $seated;
    return $this->{"seated"};
  }

sub zero
  {
    my $this=shift;

    if(seated($this))
      {
	my $slot;
	for($slot=0;$slot<HV_SLOTS_PER_MODULE;$slot++)
	  {
	    slot($this,$slot)->zero;
	  }
      }
  }

sub update
  {
    my $this=shift;
    my $updates=shift;

    my $slot;
    for($slot=0;$slot<HV_SLOTS_PER_MODULE;$slot++)
      {
	my $update=$updates->[$slot];
	next unless (defined $update);

	my $S=slot($this,$slot);
	
	demand($S,$update->{"demand"},0)
	  if((exists $update->{"demand"}) and
	     ($update->{"demand"} != demand($S)));
	
	enable($S,$update->{"enable"},0)
	  if((exists $update->{"enable"}) and
	     ($update->{"enable"} != enable($S)));
      }

    write_settings($this);
}

sub demand_and_write
  {
    my $this=shift;
    my $demands=shift;

    my $slot;
    foreach $slot ( keys %{$demands} )
      {
	slot($this,$slot)->demand($demands->{$slot});
      }

    write_settings($this,COMMUNICATE_DEMAND);
  }

sub enable_and_write
  {
    my $this=shift;
    my $enables=shift;

    my $slot;
    foreach $slot ( keys %{$enables} )
      {
	slot($this,$slot)->enable($enables->{$slot});
      }

    write_settings($this,COMMUNICATE_ENABLE);
  }

sub measure
  {
    my $this=shift;

    return unless(seated($this));

    my @tomeasure=@_;
    @tomeasure= qw(CE DV MV MC ST) if ( scalar(@tomeasure) == 0 );

    my @measures;

    foreach ( @tomeasure )
      {
	my @response=send_command($this,"RC",$_);
	if((lc $response[0] ne "rc") or
	   (lc $response[1] ne lc S($this)) or
	   (lc $response[2] ne lc $_))
	  {
	    $DEBUG_STREAM->print("Invalid response from crate:",
				 join(" ",@response),"\n");
	  }
	else
	  {
	    my $tube;
	    for($tube=0;$tube<HV_SLOTS_PER_MODULE;$tube++)
	      {
		$measures[$tube]->{lc $_}=$response[$tube+3];
	      }
	  }
      }

    my $tube;
    for($tube=0;$tube<HV_SLOTS_PER_MODULE;$tube++)
      {
	slot($this,$tube)->set_measures($measures[$tube]);
      }

    return;
  }

sub measure_and_return
  {
    my $this=shift;
    my $slot_hash=shift;

    measure($this);

    my %return;
    my $slot;
    foreach $slot ( keys %{$slot_hash} )
      {
	$return{$slot}=slot($this,$slot)->measure;
      }

    return \%return;
  }

sub display
  {
    my $this=shift;
    my $fp=shift;

    if(seated($this))
      {
	my $slot;
	for($slot=0;$slot<HV_SLOTS_PER_MODULE;$slot++)
	  {
	    $this->{"slot"}->[$slot]->display($fp);
	    $fp->print(" ") unless $slot==(HV_SLOTS_PER_MODULE-1);
	  }
      }
    else
      {
	$fp->print("No module seated in this crate slot");
      }
  }

sub S
  {
    my $this=shift;
    return sprintf("S%1d",modnum($this));
  }

sub send_command
  {
    my $this=shift;

    return unless (seated($this));

    my $cmd_action=shift;
    my $cmd_option=shift;

    my $full_command=join(" ",$cmd_action,S($this),$cmd_option);

    return crate($this)->send_command($full_command);
  }

sub write_settings
  {
    my $this=shift;
    my $writemask=shift;

    return unless (seated($this));

    if((not defined $writemask) or ($writemask & COMMUNICATE_ENABLE))
      {
	my $enables=join(" ","CE",map({ sprintf("%1d",$_->enable); }
				      @{$this->{"slot"}}));
	send_command($this,"LD",$enables);
      }

    if((not defined $writemask) or ($writemask & COMMUNICATE_DEMAND))
      {
	my $demands=join(" ","DV",map({ sprintf("%.1f",$_->demand); }
				      @{$this->{"slot"}}));
	send_command($this,"LD",$demands);
      }

    return;
  }

sub read_settings
  {
    my $this=shift;

    return unless (seated($this));

    my @enables=send_command($this,"RC","CE");
    if((lc $enables[0] ne "rc") or
       (lc $enables[1] ne lc S($this)) or
       (lc $enables[2] ne "ce"))
      {
	$DEBUG_STREAM->print("Invalid response from crate:",
			     join(" ",@enables),"\n");
	die;
      }

    my @demands=send_command($this,"RC","DV");
    if((lc $demands[0] ne "rc") or
       (lc $demands[1] ne lc S($this)) or
       (lc $demands[2] ne "dv"))
      {
	$DEBUG_STREAM->print("Invalid response from crate:",
			     join(" ",@demands),"\n");
	die;
      }

    my $slot;
    for($slot=0;$slot<HV_SLOTS_PER_MODULE;$slot++)
      {
	my $en=$enables[$slot+3];
	my $de=$demands[$slot+3];
	slot($this,$slot)->enable($en);
	slot($this,$slot)->demand($de);
      }

    return;
  }

#
# **************************** Package HV::Crate ****************************
#

#
# HV::Crate is the representation of a real HV crate, it has slots populated
# by HV::Module's. It also contains the FileHandle for the network connection
# to the crate itself. All communication to the hardware goes through this
# package, which serialises access to the crate when using threads. It also
# provides some methods that are only meaningful on the crate level, like
# turning the HV on, getting crate status etc.
#

package HV::Crate;
use FileHandle;
use IO::Socket;

BEGIN { HV::Constant->import; }
BEGIN { if ( $HV_RunningThreaded ) { require Thread; import Thread; } }

sub new
  {
    my $pkg = shift;
    my $IP = shift;

    die "Cannot clone Crate !" if(ref($pkg));

    $DEBUG_STREAM->print("Creating connection to $IP...\n");
    my $sock;

    if(HV_FAKE_IT)
      {
	$sock = new FileHandle "/dev/null","w";
      }
    else
      {
	$sock =  new IO::Socket::INET("PeerAddr" => $IP,
				      "PeerPort" => HV_MAINFRAME_PORT,
				      "Proto"    => 'tcp',
				     );
      }

    die "Socket could not be created to ".$IP.".\n Reason: ".$!."\n"
      unless $sock;

    $sock->autoflush(1);

    my $this ={ "sock"    => $sock,
		"ip"      => $IP,
	      };
    bless $this,$pkg;

    $this->{"cratelock"} = "lock" if ( $HV_RunningThreaded );

    my $modnum;
    for($modnum=0;$modnum<HV_MODULES_PER_CRATE;$modnum++)
      {
	$this->{"module"}->[$modnum]=new HV::Module $this,$modnum;
      }

    send_command($this,"OVERRIDE");

    return $this;
  }

sub module
  {
    my $this=shift;
    my $module=shift;

    return $this->{"module"}->[$module] if ((defined $module) &&
					    (($module>=0) &&
					     ($module<HV_MODULES_PER_CRATE)));

    return undef;
  }

sub ip
  {
    my $this=shift;
    return $this->{"ip"};
  }

sub cratelock
  {
    my $this=shift;
    return \$this->{"cratelock"};
  }

sub measure
  {
    my $this=shift;
    my $modnum;
    for($modnum=0;$modnum<HV_MODULES_PER_CRATE;$modnum++)
      {
	module($this,$modnum)->measure;
      }
    return;
  }

sub display
  {
    my $this=shift;
    my $fp=shift;
    my $modnum;

    $fp->print("Crate: ",$this->{"ip"},"\n");
    for($modnum=0;$modnum<HV_MODULES_PER_CRATE;$modnum++)
      {
	$fp->printf(" %2d: ",$modnum);
	$this->{"module"}->[$modnum]->display($fp);
	$fp->print("\n");
      }

    return;
  }

sub zero
  {
    my $this=shift;
    my $modnum;

    for($modnum=0;$modnum<HV_MODULES_PER_CRATE;$modnum++)
      {
	module($this,$modnum)->zero;
      }

    return;
  }

sub crate_status
  {
    my $this=shift;
    my @statusmessage=send_command($this,"CONFIG");

    if(lc $statusmessage[0] ne "config")
      {
	$DEBUG_STREAM->print("Unexpected response from crate: ",
			     join(" ",@statusmessage),"\n");
	return "BAD RESPONSE FROM CRATE";
      }

    my @response;

    my $statusbitstring=unpack("B16",pack("H4",$statusmessage[1]));
    my @statusbit=reverse $statusbitstring =~ /(.)/g;

    push @response,"* BAD EEPROM *"           if ( not $statusbit[4] );
    push @response,"* BAD INTERNAL BATTERY *" if ( not $statusbit[5] );
    push @response,"* BAD 24V STATUS *"       if ( not $statusbit[6] );

    push @response,"* power up status - not ready *"
      if ( ( not $statusbit[9] ) and ( not $statusbit[8] ) );
    push @response,"* power up status - warning *"
      if ( ( $statusbit[9] ) and ( not $statusbit[8] ) );
    push @response,"* power up status - normal *"
      if ( ( not $statusbit[9] ) and (  $statusbit[8] ) );
    push @response,"* power up status - error *"
      if ( ( $statusbit[9] ) and (  $statusbit[8] ) );

    push @response,"* HV off *"
      if ( ( not $statusbit[13] ) and ( not $statusbit[14] ) );
    push @response,"* HV on *"
      if ( ( $statusbit[13] ) and ( not $statusbit[14] ) );
    push @response,"* HV in transition *"
      if ( ( not $statusbit[13] ) and ( $statusbit[14] ) );
    push @response,"* HV status unknown ??? *"
      if ( ( not $statusbit[13] ) and ( not $statusbit[14] ) );

    push @response,"**** PANIC OFF ****" if ( $statusbit[15] );

    return @response;
  }

sub write_settings
  {
    my $this=shift;
    my $modnum;

    for($modnum=0;$modnum<HV_MODULES_PER_CRATE;$modnum++)
      {
	$this->{"module"}->[$modnum]->write_settings();
      }

    return;
  }

sub read_settings
  {
    my $this=shift;
    my $modnum;

    for($modnum=0;$modnum<HV_MODULES_PER_CRATE;$modnum++)
      {
	$this->{"module"}->[$modnum]->read_settings();
      }

    return;
  }

my @fake_data;
my $i = 0;
while($i<492)
  {
    $fake_data[$i] = { "ce" => (rand()>0.5)?1:0,
		       "dv" => -(rand()*150+800) };
    $i++;
  }

sub fake_reply  # Just for testing !!
  {
    my $message=shift;
    my @cpts=split /\s+/,$message;

    my $nlo=0;
    my $nhi=0;

    if(lc $cpts[0] eq "rc")
      {
	if($cpts[1] =~ /(\d+)[.](\d+)/)
	  {
	    $nlo = $1*12+$2;
	    $nhi = $1*12+$2;
	  }
	else
	  {
	    $cpts[1] =~ /(\d+)/;	
	    $nlo= $1*12;
	    $nhi= $1*12+11;
	  }

	if(lc $cpts[2] eq "ce")
	  {
	    if($cpts[1] =~ /[.]/)
	      {
		return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2],
			    $fake_data[$nlo]->{"ce"})."\n";
	      }
	    else
	      {
		return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2],
			    map { $_->{"ce"} } @fake_data[$nlo .. $nhi]
			   )."\n";
	      }
	  }
	elsif(lc $cpts[2] eq "st")
	  {
	    if($cpts[1] =~ /[.]/)
	      {
		return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2],
			    int(rand()*256))."\n";
	      }
	    else
	      {
		return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2],
			    int(rand()*256),int(rand()*256),int(rand()*256),
			    int(rand()*256),int(rand()*256),int(rand()*256),
			    int(rand()*256),int(rand()*256),int(rand()*256),
			    int(rand()*256),int(rand()*256),int(rand()*256)
			   )."\n";
	      }
	  }
	else
	  {
	    if($cpts[1] =~ /[.]/)
	      {
		return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2],
			    $fake_data[$nlo]->{"dv"})."\n";
	      }
	    else
	      {
		return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2],
			    map { $_->{"dv"} } @fake_data[$nlo .. $nhi]
			   )."\n";
	      }
	  }
      }
    elsif(lc $cpts[0] eq "ld")
      {
	return join(" ","    ","1",$cpts[0],$cpts[1],$cpts[2])."\n";
      }
    elsif(lc $cpts[0] eq "config")
      {
	return join(" ","    ","1","CONFIG",
		    "2174","0001","0007","0070","00AB")."\n";
      }
    elsif(lc $cpts[0] eq "hvstatus")
      {
	return join(" ","    ","1","HVSTATUS",
		    (rand()>0.5)?"HVON":"HVOFF")."\n";
      }
    else
      {
	return "    124 ERROR - BLAH |FAKE ERROR|\n";
      }
  }
	
sub send_command
  {
    my $this=shift;
    my $full_command_line=shift;

    lock cratelock($this) if ( $HV_RunningThreaded );

    $this->{"sock"}->send($full_command_line."\0",0) unless(HV_FAKE_IT);

    # VERBOSE OPERATION
    $DEBUG_STREAM->print($this->{"ip"}," << ",$full_command_line,"\n")
      if(HV_verbosity > 1);

    my $reply;

    if(HV_FAKE_IT)
      {
	$reply=fake_reply($full_command_line);
      }
    else
      {
	$reply=recv_reply($this);
      }
    chomp $reply;

    # VERBOSE OPERATION
    $DEBUG_STREAM->print($this->{"ip"}," >> ",$reply,"\n")
      if(HV_verbosity > 1);

    my @reply=split /\s+/,$reply;
    shift(@reply);
    shift(@reply);

    return @reply;
  }

sub recv_reply
  {
    my $this=shift;

    my $ok;
    my $reply;
    my $c;
    my $firstc;

    while(1)
      {
	$ok=$this->{"sock"}->recv($c,1,0);
	$firstc=$c;
	while((defined $ok) and ($c ne "\0"))
	  {
	    $reply.=$c;
	    $ok=$this->{"sock"}->recv($c,1,0);
	  }

	die("Recv failed: $!\n") unless(defined($ok));

	$reply.="\n";

	last unless($firstc eq "C")
      }

    return $reply;
  }

sub HVon
  {
    my $this=shift;
    send_command($this,"HVON");
  }

sub HVoff
  {
    my $this=shift;
    send_command($this,"HVOFF");
  }

sub HVstatus
  {
    my $this=shift;
    my @status=send_command($this,"HVSTATUS");
    return undef unless ( lc $status[0] eq "hvstatus" );
    return $status[1];
  }

#
# ************************** Package HV::Channels ***************************
#

package HV::Channels;
BEGIN { HV::Constant->import; }
BEGIN { if ( $HV_RunningThreaded ) { require Thread; import Thread; } }

sub new
  {
    my $pkg=shift;

    my $this = { "channel" => {},
		 "crate"   => {},
		 "channel_geom" => {},
	       };
    bless $this,$pkg;

    return $this;
  }

sub define_crate
  {
    my $this=shift;
    my $crate_id=shift;
    my $crate_IP=shift;

    die "Crate already defined!" if exists $this->{"crate"}->{$crate_id};

    my $C=new HV::Crate($crate_IP);
    $this->{"crate"}->{$crate_id}=$C;

    return $C;
  }


sub crate
  {
    my $this=shift;
    my $crateid=shift;
    return ( $this->{"crate"}->{$crateid} )
      if ( exists $this->{"crate"}->{$crateid} );
    return undef;
  }

sub crates
  {
    my $this=shift;
    return map { $this->{"crate"}->{$_} } sort keys %{$this->{"crate"}};
  }

sub crate_ids
  {
    my $this=shift;
    return sort keys %{$this->{"crate"}};
  }

sub channel_nos
  {
    my $this=shift;
    return sort { $a <=> $b } keys %{$this->{"channel"}};
  }

sub add_channel
  {
    my $this=shift;
    my $channel=shift;
    my $crate=shift;
    my $module=shift;
    my $slot=shift;

    die "Channel ".$channel." already defined!"
      if (exists $this->{"channel"}->{$channel});

    die "Crate undefined!" unless exists $this->{"crate"}->{$crate};
    my $C=$this->{"crate"}->{$crate};

    my $M=$C->module($module);
    die "Module out of range!" unless defined $M;
    $M->seated(1);

    my $S=$M->slot($slot);
    die "Slot out of range!" unless defined $S;

    $this->{"channel"}->{$channel}=$S;
    $this->{"channel_geom"}->{$channel}=[$crate, $module, $slot];

    return $S;
  }

sub channel
  {
    my $this=shift;
    my $channel=shift;

    unless(exists $this->{"channel"}->{$channel})
      {
	$DEBUG_STREAM->print("Channel ",$channel," not defined!\n");
	return undef;
      }
    return $this->{"channel"}->{$channel};
  }

sub channel_geom
  {
    my $this=shift;
    my $channel=shift;

    unless(exists $this->{"channel_geom"}->{$channel})
      {
	$DEBUG_STREAM->print("Channel ",$channel," not defined!\n");
	return undef;
      }
    return @{$this->{"channel_geom"}->{$channel}};
  }

sub display_by_crate
  {
    my $this=shift;
    my $fp=shift;

    my $crateid;
    foreach $crateid (sort keys %{$this->{"crate"}})
      {
	$this->{"crate"}->{$crateid}->display($fp);
      }
  }

sub display
  {
    my $this=shift;
    my $fp=shift;

    my $channel;
    foreach $channel (sort { $a <=> $b } keys %{$this->{"channel"}})
      {
	$fp->printf("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.1f\n",
		    $channel,
		    $channel,
		    $this->{"channel_geom"}->{$channel}->[0],
		    $this->{"channel_geom"}->{$channel}->[1],
		    $this->{"channel_geom"}->{$channel}->[2],
		    $channel,
		    $this->{"channel"}->{$channel}->enable,
		    $this->{"channel"}->{$channel}->demand);
      }
  }

sub AllCrates
  {
    my $this=shift;
    my $function=shift;
    my @args=@_;

    my %replies;
    my $crateid;
    if ( $HV_RunningThreaded )
      {
	my %Thread;
	foreach $crateid (sort keys %{$this->{"crate"}})
	  {
	    $Thread{$crateid} = new Thread ( $function,
					     crate($this,$crateid),
					     @args );
	  }
	
	foreach $crateid (keys %Thread)
	  {
	    my @reply=$Thread{$crateid}->join;
	    $replies{crate($this,$crateid)->ip}=\@reply;
	  }
      }
    else
      {
	foreach $crateid (sort keys %{$this->{"crate"}})
	  {
	    my @reply=&{$function}(crate($this,$crateid),@args);
	    $replies{crate($this,$crateid)->ip}=\@reply;
	  }
      }
    return \%replies;
  }

sub write_settings_to_crates
  {
    my $this=shift;
    my $channel=shift;

    if(defined $channel)
      {
	if(exists $this->{"channel"}->{$channel})
	  {
	    $this->{"channel"}->{$channel}->write_setting;
	  }
	else
	  {
	    $DEBUG_STREAM->print("write_settings_to_crate: Unknown channel ",
				 $channel,"... ignored\n");
	  }
      }
    else
      {
	AllCrates($this,\&HV::Crate::write_settings);
      }
  }

sub read_settings_from_crates
  {
    my $this=shift;

    my $channel=shift;

    if(defined $channel)
      {
	if(exists $this->{"channel"}->{$channel})
	  {
	    $this->{"channel"}->{$channel}->read_setting;
	  }
	else
	  {
	    $DEBUG_STREAM->print("read_settings_from_crate: Unknown channel ",
				 $channel,"... ignored\n");
	  }
      }
    else
      {
	AllCrates($this,\&HV::Crate::read_settings);
      }
  }

sub CertainChannels
  {
    my $this=shift;
    my $channel_function=shift;
    my $channel_data=shift;
    my $module_function=shift;
    my $module_function_cost=shift;

    my %replies;
    my %CrateModuleData;

    my $channel;
    foreach $channel ( keys %{$channel_data} )
      {
	my ($c, $m, $s)=channel_geom($this,$channel);
	next unless ( defined $c );
	$CrateModuleData{$c}->{$m}->{$s}=$channel_data->{$channel};
      }

    if ( $HV_RunningThreaded )
      {
	my %Thread;
	my $crateid;
	foreach $crateid ( keys %CrateModuleData )
	  {
	    my %crate_replies;

	    my $ModuleData=$CrateModuleData{$crateid};
	    my $crate=crate($this,$crateid);

	    $Thread{$crateid}=Thread::async
	      {
		lock $crate->cratelock;

		my $module;
		foreach $module ( keys %{$ModuleData} )
		  {
		    my $SlotData=$ModuleData->{$module};
		    my $M=$crate->module($module);
		    if((defined $module_function) and 
		       (defined $module_function_cost) and
		       (scalar(keys %{$SlotData}) >= $module_function_cost))
		      {
			# Not my favourite way to do this !!
			$replies{$crateid}->{$module}=
			  &{$module_function}($M,$ModuleData->{$module});
		      }
		    else
		      {
			my $slot;
			foreach $slot ( keys %{$SlotData} )
			  {
			    my $S=$M->slot($slot);
			    $replies{$crateid}->{$module}->{$slot}=
			      &{$channel_function}($S,$SlotData->{$slot});
			  }
		      }
		  }
		\%replies;
	      };
	  }

	foreach $crateid (keys %Thread)
	  {
	    $Thread{$crateid}->join;
	  }
      }
    else
      {
	my %Thread;
	my $crateid;
	foreach $crateid ( keys %CrateModuleData )
	  {
	    my $ModuleData=$CrateModuleData{$crateid};
	    my $crate=crate($this,$crateid);

	    my $module;
	    foreach $module ( keys %{$ModuleData} )
	      {
		my $SlotData=$ModuleData->{$module};
		my $M=$crate->module($module);
		if((defined $module_function) and
		   (defined $module_function_cost) and
		   (scalar(keys %{$SlotData}) >= $module_function_cost))
		  {
		    $replies{$crateid}->{$module}=
		      &{$module_function}($M,$ModuleData->{$module});
		  }
		else
		  {
		    my $slot;
		    foreach $slot ( keys %{$SlotData} )
		      {
			my $S=$M->slot($slot);
			$replies{$crateid}->{$module}->{$slot}=
			  &{$channel_function}($S,$SlotData->{$slot});
		      }
		  }
	      }
	  }
      }

    my %channel_replies;
    foreach $channel ( keys %{$channel_data} )
      {
	my ($c, $m, $s)=channel_geom($this,$channel);
	next unless ( defined $c );
	$channel_replies{$channel}=$replies{$c}->{$m}->{$s}
	if ( exists $replies{$c}->{$m}->{$s} );
      }

    return \%channel_replies;
  }

sub update
  {
    my $this=shift;
    my $updates=shift;
    my @updated;

    my $channel;

    my %enables=();
    my %demands=();

    foreach $channel ( keys %{$updates} )
      {
	$enables{$channel}=$updates->{$channel}->{"enable"}
	if ( exists $updates->{$channel}->{"enable"} );

	$demands{$channel}=$updates->{$channel}->{"demand"}
	if ( exists $updates->{$channel}->{"demand"} );
      }

    CertainChannels($this,\&HV::Slot::enable_and_write,\%enables,
		    \&HV::Module::enable_and_write,2);
    CertainChannels($this,\&HV::Slot::demand_and_write,\%demands,
		    \&HV::Module::demand_and_write,4);

    return sort { $a <=> $b } keys %{$updates};
  }

sub measure
  {
    my $this=shift;
    my @channels=@_;
    my @updated;

    if(scalar(@channels) == 0)
      {
	AllCrates($this,\&HV::Crate::measure);
	@channels=
	
	my %measures;
	my $channel;
	foreach $channel (channel_nos($this))
	  {
	    my $C=channel($this,$channel);
	    $measures{$channel}=$C->measure;
	  }

	return \%measures;
      }
    else
      {
	my %measure_channels;
	my $channel;
	foreach $channel ( @channels )
	  {
	    $measure_channels{$channel}=undef;
	  }
	my $measures=CertainChannels($this,
				     \&HV::Slot::measure,\%measure_channels,
				     \&HV::Module::measure_and_return,4);

	return $measures;
      }
  }

sub HVon
  {
    my $this=shift;
    AllCrates($this,\&HV::Crate::HVon);
  }

sub HVoff
  {
    my $this=shift;
    AllCrates($this,\&HV::Crate::HVoff);
  }

sub HVstatus
  {
    my $this=shift;
    my %status;
    return AllCrates($this,\&HV::Crate::HVstatus)
  }

sub HVcratestatus
  {
    my $this=shift;
    my %status;
    return AllCrates($this,\&HV::Crate::crate_status)
  }

sub add_channels_from_hv_settings_file
  {
    my $this=shift;
    my $filename=shift;

    my $fp=new FileHandle $filename,"r";
    return undef unless ( defined $fp );

    while(defined($_=$fp->getline))
      {
	chomp;
	s/^\s*(.*)\s*$/$1/;
	next if ((/^$/) or (/^\#/));
	my ($channel,$a,$C,$M,$S,$b,$enabled,$demand)=split /\s+/;
	$this->add_channel($channel,$C,$M,$S);
      }

    return 1;
  }

sub read_settings_from_file
  {
    my $this=shift;
    my $filename=shift;

    my $fp=new FileHandle $filename,"r";
    unless(defined $fp)
      {
	$DEBUG_STREAM->print("Cannot find file $filename... ",
			     "no action taken!\n");
	return undef;
      }

    my $C;
    foreach $C (crates($this))
      {
	$C->zero;
      }

    $this->{"channel_geom"}={};
    $this->{"channel"}={};

    while(defined($_=$fp->getline))
      {
	chomp;
	s/^\s*(.*)\s*$/$1/;
	next if ((/^$/) or (/^\#/));
	my ($channel,$a,$C,$M,$S,$b,$enabled,$demand)=split /\s+/;
	my $Slot=add_channel($this,$channel,$C,$M,$S);
	$Slot->demand($demand);
	$Slot->enable($enabled);
      }

    return 1;
  }

my %STATES;
sub STATEremember
  {
    my $this=shift;
    my $name=shift;

    my $state={};
    $STATES{$name} = $state  if(defined $name);

    my $channel;
    foreach $channel ( channel_nos($this) )
      {
	$state->{$channel} = { "demand" => channel($this,$channel)->demand,
			       "enable" => channel($this,$channel)->enable };
      }

    return $state;
  }

sub STATErestore
  {
    my $this=shift;
    my $name=shift;

    return undef unless (defined $name);

    my $state;
    if(ref $name) { $state=$name }
    elsif(exists $STATES{$name}) { $state=$STATES{$name} }
    else { return undef }

    return update($this,$state);
  }

sub STATE
  {
    my $this=shift;
    my $name=shift;
    my $channel=shift;

    return undef unless (defined $name);

    my $state;
    if(ref $name) { $state=$name }
    elsif(exists $STATES{$name}) { $state=$STATES{$name} }
    else { return undef }

    return undef unless (exists $state->{$channel});

    return $state->{$channel};
  }

sub STATEforget
  {
    my $this=shift;
    my $name=shift;

    return undef unless (defined $name);
    return $name if (ref $name);
    return undef unless (exists $STATES{$name});
    return delete $STATES{$name};
  }

sub STATElist
  {
    my $this=shift;

    return(sort keys %STATES);
  }

#
#
#

package main;
BEGIN { HV::Constant->import; }

use Text::Abbrev;
use Text::Wrap;
use Term::ReadLine;

sub UnBunchNos
  {
    my %Nos;

    my $RunNos;
    while (defined($RunNos=shift @_))
      {
	my @UnBunched=split(/\s*([-,\s;])\s*/,$RunNos);
	my ($r,$op);
	
	while($r=shift(@UnBunched))
	  {
	    next unless $r=~/^\d+$/;

	    $op=shift(@UnBunched);
	    if((not $op)or($op eq ";")or($op eq ",")or($op=~/\s/))
	      {
		$Nos{$r}=1;
	      }
	    elsif($op eq "-")
	      {
		my $f=shift(@UnBunched);
		if($f=~/^\d+$/)
		  {
		    $Nos{$r++}=1 while($r<=$f);
		  }
		$op=shift(@UnBunched);
	      }
	  }
      }

    return sort({ $a <=> $b } keys %Nos);
  }

sub info
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my @args=@_;

    return if ( ( not defined $args[0] ) and ( lc $command ne "measure" ) );

    if ( lc $command eq "measure" )
      {
	my $measures=$HV->measure(UnBunchNos(@_));
	
	my $channel;
	foreach $channel ( sort { $a <=> $b } keys %{$measures} )
	  {
	    my $measures=$measures->{$channel};
	    my $status=HV::Slot::slot_status($measures->{"st"});
	    $OUT->printf("Channel %3d is %-8.8s, ",
			 $channel,
			 ($measures->{"ce"})?"enabled":"disabled");
	    if(($status->{"tripped"}) or
	       (($status->{"enabled"}==0) and
		($HV->channel($channel)->enable==1)))
	      {
		$OUT->print($status->{"text"},"\n");
	      }
	    else
	      {
		$OUT->printf("demand %7.1f, ".
			     "measured %7.1fV, %7.1fuA\n",
			     $measures->{"dv"},$measures->{"mv"},
			     $measures->{"mc"});
	      }
	  }
      }
    elsif(lc $args[0] eq "off")
      {
	my $channel;
	my $string="";
	foreach $channel ( $HV->channel_nos )
	  {
	    my $C=$HV->channel($channel);
	    $string .= $channel." "
	      if(not $C->enable);
	  }
	$OUT->print(wrap("Disabled channels: ","   ",$string),"\n");
	
	$string="";
	foreach $channel ( $HV->channel_nos )
	  {
	    my $C=$HV->channel($channel);
	    $string .= $channel." " 
	      if((not $C->enable) and 
		 ($HV->STATE("default",$channel)->{"enable"}));
	  }
	$OUT->print(wrap("Disabled since last \"remember\": ","   ",$string),
		    "\n") if ( $string );
      }
    else
      {
	my $channel;
	foreach $channel ( UnBunchNos(@_) )
	  {
	    my $C=$HV->channel($channel);

	    if(defined $C)
	      {
		$OUT->print("Channel ",$channel,
			    " voltage ",$C->demand," , ",
			    ($C->enable)?"Enabled":"Disabled","\n");
	      }
	  }
      }
  }

sub set
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my $change=pop;
    my @channels=UnBunchNos(@_);

    my %updates;
    my %olds;
    my $channel;

    return if ( (not defined $change) or (scalar(@channels) == 0) );

    foreach $channel (@channels)
      {
	my $C=$HV->channel($channel);
	$olds{$channel}=$C->demand;

	if(defined $C)
	  {
	    my $de=$change;
	    if($de =~ /([+-])([1-90]*)/)
	      {
		$de=$C->demand;
		if($1 eq "+") { $de -= $2 }
		else { $de += $2 };
	      }
	    else
	      {
		$de=-$de;
	      }
	    $updates{$channel}->{"demand"}=$de;
	  }
      }

    foreach $channel (sort { $a <=> $b } $HV->update(\%updates))
      {
	my $C=$HV->channel($channel);
	$OUT->print("Channel ",$channel," was ",$olds{$channel});
	$OUT->print(" now ",$C->demand,"\n");
      }
  }

sub toggle
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my @args=UnBunchNos(@_);

    my %update;

    foreach $_ ( @args )
      {
	my $C=$HV->channel($_);
	
	if(defined $C)
	  {
            my $en=$C->enable;
	    if(($command eq "on") or ($command eq "enable"))
	      {
		$en=0;
	      }
	    elsif(($command eq "off") or ($command eq "disable"))
	      {
		$en=1;
	      }

	    $update{$_}->{"enable"}=($en==1)?0:1,1;
	  }
      }

    my @updated=$HV->update(\%update);

    foreach ( sort { $a <=> $b } @updated )
      {
	my $C=$HV->channel($_);
	$OUT->print("Channel ",$_," now ",
		    ($C->enable)?"Enabled":"Disabled","\n");
      }
  }

sub hv
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my $what=shift;

    $what="" if (lc $command eq "cratestatus");
    $what="status" unless(defined $what);

    if($what eq "ON")
      {
	$HV->HVon;
	$what="status";
      }
    elsif(lc $what eq "on")
      {
	$OUT->print("-- To turn the crates on you must say \"hv ON\", ie.\n",
		    "-- the ON must be capitals. This is a useless safety\n",
		    "-- feature!\n");
      }
    elsif(lc $what eq "off")
      {
	$HV->HVoff;
	$what="status";
      }
    elsif((lc $what eq "cratestatus") or (lc $command eq "cratestatus"))
      {
	my $REPLIES=$HV->HVcratestatus;
	my $crateid;
	foreach $crateid (sort keys %{$REPLIES})
	  {
	    my $resp;
	    foreach $resp (@{$REPLIES->{$crateid}})
	      {
		$OUT->print($crateid,": ",$resp,"\n");
	      }
	  }
      }

    if(lc $what eq "status")
      {
	my $REPLIES=$HV->HVstatus;
	$OUT->print(join("\n",map({ $_.": ".
				    ((defined $REPLIES->{$_}->[0])?
				     $REPLIES->{$_}->[0]:"ERROR") }
				  sort keys %{$REPLIES}),""));
      }
  }
	
sub write
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my $filename=shift;

    my $fp;
    if(defined $filename)
      {
	$fp=new FileHandle;
	$fp->open($filename,"w");
	if(not defined $fp)
	  {
	    $OUT->print("Could not open ",$filename," for writing.\n");
	    $OUT->print("Error: ",$!,"\n");
	    return;
	  }
      }
    else
      {
	$fp=$OUT;
      }

    $HV->display($fp);
  }

sub read
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my $filename=shift;

    if(defined $filename)
      {
	if($HV->read_settings_from_file($filename))
	  {
	    $HV->write_settings_to_crates;
	  }
      }
    else
      {
	$OUT->print("Need a filename !!\n");
      }
  }

sub state
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    $command=shift if(lc $command eq "state");

    my $name=shift;

    $name="default" unless defined $name;

    if($command eq "remember")
      {
	my $channel;
	$HV->STATEremember($name);
	$OUT->print("State saved.. use \"restore",
		    ($name eq "default")?"":" ".$name,
		    "\" to recall ",
		    "current settings\n");
      }
    elsif($command eq "restore")
      {
	my @updated=$HV->STATErestore($name);
	return unless(defined $updated[0]);
	
	foreach(sort { $a <=> $b } @updated)
	  {
	    my $C=$HV->channel($_);
	    $OUT->print("Channel ",$_,
			" voltage ",$C->demand," , ",
			($C->enable)?"Enabled":"Disabled","\n");
	  }
      }
    elsif($command eq "forget")
      {
	$HV->STATEforget($name);
      }
    elsif($command eq "list")
      {
	$OUT->print(join("\n  ","States stored:",$HV->STATElist),"\n");
      }
  }

sub quit
  {
    my $term=shift;
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;

    if(grep { /on/i } map { $_->[0] } values %{$HV->HVstatus})
      {
	while(1)
	  {
	    $OUT->print("HV is still ON, do you really want to quit: ");
	    my $response=
	      $term->readline("","no");

	    if($response =~ /^y(es)?$/)
	      {
		last;
	      }
	    elsif($response =~ /^no?$/)
	      {
		return;
	      }
	    else
	      {
		$OUT->print("Just a YES or a NO please\n");
	      }
	  }
      }
    $OUT->print("bye....\n");
    exit;
  }

sub help
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;

    $OUT->print(join("\n",
"Commands available are:",
"info <channel> ...      - Show channel information",
"set <channel> <voltage> - Set channel demand voltage. <voltage> is of the",
"                          form VVVV.V, +VVVV.V or -VVVV.V where +/- make",
"                          the, voltage more NEGATIVE and POSITIVE",
"                          respectively",
"on <channel> ...        - enable channel",
"off <channel> ...       - disable channel",
"toggle  <channel> ...   - toggle channel",
"write [filename]        - save settings to filename or to screen",
"read [filename]         - read HV settings from file",
"display [filename]      - same as write",
"verbose <level>         - set verbose level (2 shows all network traffic)",
"hv <ON|off|status>      - turn HV on/off or show status",
"gainmatch               - an aid to gain matching",
"remember                - remember current settings",
"restore                 - restore \"remembered\" settings",
		     ""));
  }

sub verbose
  {
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;
    my $verbosity=shift;

    $verbosity=2 unless ( defined $verbosity );

    HV::Constant::HV_verbosity($verbosity);
  }

sub GMget_maxvchange
  {
    my $term=shift;
    my $OUT=shift;
    my $response=shift;
    my $maxvchange;

    while(1)
      {
	unless(defined($response))
	  {
	    $OUT->print("Whats the largest voltage change i can make ",
			"in one go: ");
	    $response=lc $term->readline("","50");
	  }
	
	if(($response =~ /^\d+([.]\d+)?$/) && 
	   ($response >= 1) && ($response <= 100))
	  {
	    $maxvchange=$response;
	    last;
	  }
	else
	  {
	    $OUT->print("Please enter a number between 1 and 100\n");
	  }

	undef $response;
      }
    $OUT->print("\n");

    return $maxvchange;
  }

sub GMget_latitude
  {
    my $term=shift;
    my $OUT=shift;
    my $response=shift;
    my $latitude;

    while(1)
      {
	unless(defined($response))
	  {
	    $OUT->print("How close to the rep. channel do i have to get ? ");
	    $response=lc $term->readline("");
	  }
	
	if(($response =~ /^\d+([.]\d+)?$/) && ($response >= 0))
	  {
	    $latitude=$response;
	    last;
	  }
	else
	  {
	    $OUT->print("Enter a value >= 0\n");
	  }
	undef $response;
      }
    $OUT->print("\n");

    return $latitude;
  }

sub GMget_repchannel
  {
    my $term=shift;
    my $OUT=shift;
    my $HV=shift;
    my $response=shift;

    my $repchannel;

    $OUT->print("In your measure, find a channel that is representative\n",
		"of a GOOD channel. I'll keep the voltage on this channel\n",
		"fixed and aim to match the measures of the others to it.\n")
      unless(defined $response);

    while(1)
      {
	unless(defined($response))
	  {
	    $OUT->print("Enter channel number: ");
	    $response=lc $term->readline("");
	  }
	
	if(defined $HV->channel($response))
	  {
	    $repchannel=$response;
	    last;
	  }
	else
	  {
	    $OUT->print("Channel \"$response\" undefined\n");
	  }

	undef $response;
      }
    $OUT->print("\n");
  }

sub gainmatch
  {
    my $term=shift;
    my $OUT=shift;
    my $HV=shift;
    my $command=shift;

    my @UNDO;

    my $response;

    $OUT->print(join("\n",
		     "Hello and welcome to the gain matching bit. Perpare to",
		     "spend ages doing dull, tedious and repetative work",
		     "The basic proceedure is that you are going to give me",
		     "a measure of the \"gains\" (eg. quicklook analysis of",
		     "a N2 run, scalars values etc...) and i'll use it to",
		     "increase and decrease voltages until all the channels",
		     "are within a certain margin or no more adjustments can",
		     "be made.... sounds like fun ? well it isn't.",
		     "",""));

    my $INCREASING;
    while(1)
      {
	$OUT->print("Is this measure an INCREASING function of VOLTAGE ? ");
	$response=lc $term->readline("");
	
	if($response =~ /^y(es)?$/)
	  {
	    $INCREASING=1;
	    last;
	  }
	elsif($response =~ /^no?$/)
	  {
	    $INCREASING=-1;
	    last;
	  }
	else
	  {
	    $OUT->print("Just a YES or a NO please\n");
	  }
      }
    $OUT->print("\n");

    my $repchannel=GMget_repchannel($term,$OUT,$HV);
#    my $latitude=GMget_latitude($term,$OUT);
    my $maxvchange=GMget_maxvchange($term,$OUT);

    my $minvchange=1;

    my $LastAdjustments;
    my $channel;

    foreach $channel ( $HV->channel_nos )
      {
	$LastAdjustments->{$channel} = "start";
	$LastAdjustments->{$channel} = undef
	  unless ( $HV->channel($channel)->enable );
      }

    my %CommandCompletions;
    abbrev(\%CommandCompletions,qw(iterate undo disable adjust show quit
				   revert reinitialise tolerence maxvchange
				   repchannel measure));

    while(1)
      {
	$OUT->print(join("\n","",
"Commands Available",
"------------------",
"iterate <filename>       - run next iteration with measure from <filename>",
"undo                     - undo last iteration",
"revert                   - revert to original voltages",
"disable <channel> ...    - stop adjusting <channel>",
"adjust <channel> <V>     - adjust voltage on <channel> by <V> (>0 negative)",
"reinitialise             - start next itteration with changes of vmax",
"maxvchange               - set maximum change",
#"tolerence                - set tolerence",			
"show <channel> ...       - show channel status and voltage",
"measure <channel> ...    - show measured channel status and voltage",
"quit                     - stop gain adjustment",
			 "",""));
	
	$response=lc $term->readline("GAINS>");
	$response =~ s/^\s+(.*)\s+$/$1/;
	
	my @components=split /\s+/,$response;
	my $command=shift @components;

	next unless ( defined $command );

	unless ( exists $CommandCompletions{$command} )
	  {
	    $OUT->print("Unknown command or ambiguous abbreviation: ",
			$command,"\n");
	    next;
	  }
	$command=$CommandCompletions{$command};
	
	if($command eq "iterate")
	  {
	    my $filename=shift @components;
	    my $channels_to_adjust=0;

	    my $fp;
	    $fp=new FileHandle $filename,"r" if (defined $filename);

	    if(defined $fp)
	      {
		my @measure;
		$OUT->print("Reading file ..... ");
		while(defined($_=$fp->getline))
		  {
		    chomp;
		    s/^\s+//;
		    push @measure,split(/\s+/,$_) unless /^\#/;
		  }
		$OUT->print(scalar(@measure)," entries\n");

		my $Adjustment={};
		my $channel;
		foreach $channel (keys %{$LastAdjustments})
		  {
		    $Adjustment->{$channel}=undef;
		    next if(not defined $LastAdjustments->{$channel});
#		    next if(($LastAdjustments->{$channel} ne "start") and
#			    (abs($LastAdjustments->{$channel}) < $minvchange));
print STDERR "xxx","\n";
		    my $ThisChanMeasureDiff;
		    if(HV_FAKE_IT)
		      {
			$ThisChanMeasureDiff=1.0;
			$ThisChanMeasureDiff=
			  exp(log($HV->channel($repchannel)->demand/
				  $HV->channel($channel)->demand)*8)
			  if(($HV->channel($repchannel)->demand != 0)&&
			     ($HV->channel($channel)->demand != 0));
		      }
		    else
		      {
			$ThisChanMeasureDiff=
			  $measure[$channel-1]/$measure[$repchannel-1];
		      }
#		    next if(abs($ThisChanMeasureDiff-1) <= $latitude);

		    my $LOG_MEA=($ThisChanMeasureDiff > 0);
		    my $LOG_INC=($INCREASING > 0);

		    $channels_to_adjust++;

# =============================================================================
#		    USE BINARY SEARCH TYPE METHOD
# =============================================================================

#		    if($LastAdjustments->{$channel} eq "start")
#		      {
#			$Adjustment->{$channel} = -$maxvchange;
#			$Adjustment->{$channel} = $maxvchange 
#			  if ($LOG_MEA ^ $LOG_INC);
#			next;
#		      }

#		    my $LOG_LST=($LastAdjustments->{$channel} > 0);

#		    my $laval=abs($LastAdjustments->{$channel});
#		    $laval /= 2.0 if ( (($LOG_LST ^ $LOG_MEA)) );
#		    $laval = -$laval if ( not ( $LOG_MEA ));

#		    $Adjustment->{$channel}=$laval;

# =============================================================================
#		    USE MORE SENSABLE 8TH-POWER GAIN TO VOLTAGE RELATIONSHIP
# =============================================================================

print STDERR $ThisChanMeasureDiff;

		    $Adjustment->{$channel}=
		      $HV->channel($channel)->demand*
			(exp(log($ThisChanMeasureDiff)*$INCREASING/8.0)-1);

		    if($Adjustment->{$channel} > $maxvchange)
		      { $Adjustment->{$channel} = $maxvchange; }
		    elsif($Adjustment->{$channel} < -$maxvchange)
		      { $Adjustment->{$channel} = -$maxvchange; }
		  }
		my %updates;

		$OUT->print("Found ",$channels_to_adjust,
			    " channels to adjust\n");

		if($channels_to_adjust > 0)
		  {
		    push(@UNDO,
			 { "last_adjustment" => $LastAdjustments,
			   "voltage_state"   => $HV->STATEremember });
		    $LastAdjustments=$Adjustment;
		    foreach $channel (grep { defined $Adjustment->{$_} }
				      sort { $a <=> $b } keys %{$Adjustment})
		      {
			my $C=$HV->channel($channel);
			$updates{$channel}->{"demand"}=
			  $C->demand-$Adjustment->{$channel};
		      }
		
		    my @updated=$HV->update(\%updates);

		    foreach $channel ( sort { $a <=> $b } @updated )
		      {
			my $C=$HV->channel($channel);
			$OUT->printf("Channel %d changed by %+dV, ".
				     "now at %dV\n",
				     $channel,-$Adjustment->{$channel},
				     $C->demand);
		      }
		  }
	      }
	    elsif(defined $filename)
	      {
		$OUT->print("Error opening $filename: $!\n");
	      }
	    else
	      {
		$OUT->print("No filename entered for measure data\n");
	      }		
	  }
	elsif ($command eq "undo")
	  {
	    my $settings=pop(@UNDO);
	    if(defined $settings)
	      {
		$LastAdjustments=$settings->{"last_adjustment"};
		$HV->STATErestore($settings->{"voltage_state"});
	      }
	    else
	      {
		$OUT->print("No more undos !\n");
	      }
	  }
	elsif ($command eq "revert")
	  {
	    my $settings=$UNDO[0];
	    @UNDO=();
	    if(defined $settings)
	      {
		$LastAdjustments=$settings->{"last_adjustment"};
		$HV->STATErestore($settings->{"voltage_state"});
	      }
	  }
	elsif ($command eq "reinitialise")
	  {
	    foreach $channel ( $HV->channel_nos )
	      {
		$LastAdjustments->{$channel} = "start";
		$LastAdjustments->{$channel} = undef
		  unless ( $HV->channel($channel)->enable );
	      }
	  }
#	elsif ($command eq "tolerence")
#	  {
#	    $latitude=GMget_latitude($term,$OUT,$components[0]);
#	    $OUT->print("Tolerence set to: ",$latitude,"\n");
#	  }
	elsif ($command eq "repchannel")
	  {
	    $repchannel=GMget_repchannel($term,$OUT,$HV,$components[0]);
	    $OUT->print("Using channel ",$repchannel,"as reference\n");
	  }
	elsif ($command eq "maxvchange")
	  {
	    $maxvchange=GMget_maxvchange($term,$OUT,$components[0]);
	    $OUT->print("Max voltage change: ",$maxvchange,"\n");
	  }
	elsif ($command eq "disable")
	  {
	    my $channel;
	    foreach $channel (UnBunchNos(@components))
	      {
		if(exists $LastAdjustments->{$channel})
		  {
		    $LastAdjustments->{$channel}=0;
		    $OUT->print("Channel ",$channel," will not be adjusted\n");
		  }
		else
		  {
		    $OUT->print("Unknown channel: ",$channel,"\n");
		  }
	      }
	  }
	elsif ($command eq "show")
	  {
	    info($OUT,$HV,$command,@components);
	  }
	elsif ($command eq "measure")
	  {
	    info($OUT,$HV,$command,@components);
	  }
	elsif ($command eq "adjust")
	  {
	    my $channel=shift @components;
	    my $voltage=shift @components;

	    if(exists $LastAdjustments->{$channel})
	      {
		if($voltage =~ /^-?\d+([.]\d+)?$/)
		  {
		    my $C=$HV->channel($channel);
		    $OUT->print("Channel ",$channel," was ",$C->demand,"V");
		    $C->demand($C->demand-$voltage);
		    $C->write_setting;
		    $OUT->print(" now ",$C->demand,"V\n");
		    $LastAdjustments->{$channel}=-$voltage;
		  }
		else
		  {
		    $OUT->print("Voltage must be a number: ",$channel,"\n");
		  }
	      }
	    else
	      {
		$OUT->print("Unknown channel: ",$channel,"\n");
	      }
	  }
	else
	  {
	    return;
	  }
      }
  }

my %Commands = ( "info"        => \&info,
		 "show"        => \&info,
		 "measure"     => \&info,
		 "set"         => \&set,
		 "on"          => \&toggle,
		 "off"         => \&toggle,
		 "disable"     => \&toggle,
		 "enable"      => \&toggle,
		 "toggle"      => \&toggle,
		 "read"        => \&read,
		 "write"       => \&write,
		 "display"     => \&write,
		 "quit"        => \&quit,
		 "exit"        => \&quit,
		 "hv"          => \&hv,
		 "cratestatus" => \&hv,
		 "verbose"     => \&verbose,
		 "gainmatch"   => \&gainmatch,
		 "remember"    => \&state,
		 "restore"     => \&state,
		 "state"       => \&state,
		 "help"        => \&help,
		 "?"           => \&help,
	       );

my %CommandDoesInput = ( "gainmatch" => 1,
			 "quit"      => 1,
		       );
		 
my %CommandShorts;
abbrev(\%CommandShorts,keys %Commands);

my $term=new Term::ReadLine 'HV';
my $OUT = new FileHandle;
$OUT->fdopen($term->OUT || *STDOUT,"w");
$OUT->autoflush(1);

#HV::Constant::HV_verbosity(2);

my $HV=new HV::Channels;

$OUT->print("Setting up connections to crates....\n");
$HV->define_crate(0,"192.33.141.35");
$HV->define_crate(1,"192.33.141.34");
$HV->define_crate(2,"192.33.141.32");

$OUT->print("\nCrate status:\n");
hv($OUT,$HV,"hv","cratestatus");

my $HVfile="/home/observer/HV/settings/hv.settings";
#my $HVfile="hv.settings";
$OUT->print("\nReading channel geometry from: ",$HVfile,"\n");
unless( defined $HV->add_channels_from_hv_settings_file($HVfile) )
  {
    $OUT->print($HVfile," unreadable! ",$!,"\n");
    $HVfile="hv.settings";
    $OUT->print("Trying: ",$HVfile,"\n");
    unless( defined $HV->add_channels_from_hv_settings_file($HVfile) )
      {
	$OUT->print($HVfile," unreadable! ",$!," exiting...\n");
	die;
      }
  }

$OUT->print("\nReading current settings from crates...\n");
$HV->read_settings_from_crates();

state($OUT,$HV,"remember");

while(1)
  {
    my $commandline=$term->readline("HV>");

    if (not defined $commandline )   # Ctrl-d
      {
	$OUT->print("\n");
	$commandline="quit\n";
      }

    chomp $commandline;
    my @components=split(/\s+/,$commandline);
    my $command=lc shift @components;

    next unless ($command);

    if(exists $CommandShorts{$command})
      {
	$command=$CommandShorts{$command}
      }
    else
      {
	$OUT->print("Unknown command or ambiguous abbreviation: ",
		    $command,"\n");
	next;
      }

    if(exists $CommandDoesInput{$command})
      {
	&{$Commands{$command}}($term,$OUT,$HV,$command,@components);
      }
    else
      {
	&{$Commands{$command}}($OUT,$HV,$command,@components);
      }
  }

1;
